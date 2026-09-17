package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"minidist/internal/chunk"
	"net/http"
	"strings"
	"time"
)

// handleInternalChunk 分发内部 chunk 的上传和下载请求。
func (n *Node) handleInternalChunk(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/internal/chunks/")
	if id == "" {
		http.Error(w, "empty chunk id", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		n.handleInternalChunkPut(w, r, id)
	case http.MethodGet:
		n.handleInternalChunkGet(w, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleInternalChunkPut 校验 chunk ID 后把原始数据保存到本地 chunk store。
func (n *Node) handleInternalChunkPut(w http.ResponseWriter, r *http.Request, id string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read chunk failed", http.StatusBadRequest)
		return
	}

	actualID := chunk.ID(data)
	if actualID != id {
		http.Error(w, "chunk checksum mismatch", http.StatusBadRequest)
		return
	}

	storedID, err := n.chunks.Put(data)
	if err != nil {
		http.Error(w, "store chunk failed", http.StatusInternalServerError)
		return
	}

	if storedID != id {
		http.Error(w, "chunk id mismatch", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleInternalChunkGet 从本节点读取指定 chunk 并返回原始二进制数据。
func (n *Node) handleInternalChunkGet(w http.ResponseWriter, id string) {
	data, err := n.chunks.Get(id)
	if errors.Is(err, chunk.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	if _, err := w.Write(data); err != nil {
		return
	}
}

// PutChunk 根据内容生成 chunk ID，并把 chunk 并发写入 ring 选出的副本。
func (n *Node) PutChunk(ctx context.Context, data []byte) (string, error) {
	id := chunk.ID(data)

	replicas := n.replicasFor(id)
	if len(replicas) == 0 {
		return "", fmt.Errorf("no chunk replicas")
	}

	results := make(chan writeResult, len(replicas))

	for _, replica := range replicas {
		go func(replica string) {
			err := n.putChunkReplica(ctx, replica, id, data)
			results <- writeResult{
				replica: replica,
				err:     err,
			}
		}(replica)
	}

	success := 0

	for range replicas {
		result := <-results
		if result.err != nil {
			continue
		}

		success++
	}

	if success < n.writeQuorum {
		return "", fmt.Errorf("chunk write quorum not reached: success=%d, required=%d", success, n.writeQuorum)
	}

	return id, nil
}

// putChunkReplica 把一个 chunk 写入指定的本地或远端副本。
func (n *Node) putChunkReplica(ctx context.Context, replica string, id string, data []byte) error {
	if replica == n.addr {
		storedID, err := n.chunks.Put(data)
		if err != nil {
			return err
		}

		if storedID != id {
			return fmt.Errorf("chunk id mismatch: expected=%s, actual=%s", id, storedID)
		}

		return nil
	}

	url := fmt.Sprintf("http://%s/internal/chunks/%s", replica, id)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("chunk replica %s returned %s", replica, resp.Status)
	}

	return nil
}

// GetChunk 从 chunk 的负责副本中读取一个校验正确的副本。
func (n *Node) GetChunk(ctx context.Context, id string) ([]byte, error) {
	replicas := n.replicasFor(id)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("no chunk replicas")
	}

	var lastErr error

	for _, replica := range replicas {
		data, err := n.getChunkReplica(ctx, replica, id)
		if err != nil {
			lastErr = err
			continue
		}

		if chunk.ID(data) != id {
			lastErr = fmt.Errorf("chunk checksum mismatch: %s", id)
			continue
		}

		// repair 不能绑定请求 context，否则请求结束后 context 会立即取消。
		repairData := bytes.Clone(data)
		repairReplicas := append([]string(nil), replicas...)
		go n.repairChunk(id, repairData, replica, repairReplicas)

		return data, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("chunk unavailable: %w", lastErr)
	}

	return nil, chunk.ErrNotFound
}

// getChunkReplica 从指定的本地或远端副本读取 chunk。
func (n *Node) getChunkReplica(ctx context.Context, replica string, id string) ([]byte, error) {
	if replica == n.addr {
		return n.chunks.Get(id)
	}

	url := fmt.Sprintf("http://%s/internal/chunks/%s", replica, id)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, chunk.ErrNotFound
	}

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("chunk replica %s returned %s", replica, resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if chunk.ID(data) != id {
		return nil, fmt.Errorf("chunk checksum mismatch from replica %s", replica)
	}

	return data, nil
}

// repairChunk 用已校验正确的数据修复缺失或损坏的负责副本。
func (n *Node) repairChunk(id string, data []byte, source string, replicas []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, replica := range replicas {
		if replica == source {
			continue
		}

		// 当前副本已有正确 chunk 时跳过。
		if _, err := n.getChunkReplica(ctx, replica, id); err == nil {
			continue
		}

		if err := n.putChunkReplica(ctx, replica, id, data); err != nil {
			log.Printf("repair chunk %s on %s: %v", id, replica, err)
		}
	}
}
