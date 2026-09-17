package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"minidist/internal/hashring"
	"minidist/internal/store"
	"net/http"
)

// goroutine 给 Coordinator 汇报结果 channel 数据结构
type writeResult struct {
	replica string
	err     error
}

type readResult struct {
	replica string
	value   store.Value
	found   bool
	err     error
}

// newer 判断 a 的版本是否严格新于 b。
func newer(a, b store.Value) bool {
	return store.CompareVersion(
		a.Version,
		b.Version,
	) > 0
}

// handleReplicatedGet 是 HTTP 请求翻译层，将 GET 请求映射为 quorum 读取和读修复。
func (n *Node) handleReplicatedGet(w http.ResponseWriter, r *http.Request, key string) {
	data, found, err := n.Get(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	_, _ = w.Write(data)
}

// replicateValue 并发写入所有副本，并为失败副本保存 hint。
// 未达到 quorum 时返回 false。
func (n *Node) replicateValue(ctx context.Context, key string, value store.Value) bool {
	replicas := n.replicasFor(key)

	results := make(chan writeResult, len(replicas))
	for _, replica := range replicas {
		go func(replica string) {
			err := n.putReplica(ctx, replica, key, value)
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
			n.hints.Add(result.replica, key, value)
			continue
		}
		success++
	}

	return success >= n.writeQuorum
}

// handleReplicatedPut 是 HTTP 请求翻译层，将 PUT 请求映射为 quorum 写入。
func (n *Node) handleReplicatedPut(w http.ResponseWriter, r *http.Request, key string) {
	// 先把请求读到内存，防止后续读 EOF
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed:", http.StatusBadRequest)
		return
	}

	if err := n.Put(r.Context(), key, data); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleReplicatedDelete 是 HTTP 请求翻译层，将 DELETE 请求映射为 tombstone quorum 写入。
func (n *Node) handleReplicatedDelete(w http.ResponseWriter, r *http.Request, key string) {
	if err := n.Delete(r.Context(), key); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// putReplica 写入本地或远端的单个副本。
func (n *Node) putReplica(ctx context.Context, replica string, key string, value store.Value) error {
	if replica == n.addr {
		return n.store.Set(key, value)
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/internal/kv/%s", replica, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("replica %s returned %s", replica, resp.Status)
	}

	return nil
}

// repairReplica 尝试将最新值回写到落后副本。
func (n *Node) repairReplica(ctx context.Context, replica string, key string, value store.Value) {
	err := n.putReplica(ctx, replica, key, value)
	if err != nil {
		log.Printf("repair replica %s failed: %v", replica, err)
	}
}

// getReplica 读取本地或远端的单个副本。
func (n *Node) getReplica(ctx context.Context, replica string, key string) (store.Value, bool, error) {
	if replica == n.addr {
		value, ok := n.store.Get(key)
		if !ok {
			return store.Value{}, false, nil
		}

		return value, true, nil
	}

	url := fmt.Sprintf("http://%s/internal/kv/%s", replica, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return store.Value{}, false, err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return store.Value{}, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return store.Value{}, false, nil
	}

	if resp.StatusCode > 300 {
		return store.Value{}, false, fmt.Errorf("replica %s returned %s", replica, resp.Status)
	}
	var value store.Value
	if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
		return store.Value{}, false, err
	}

	return value, true, nil
}

// replicasFor 返回当前 ring 中负责 key 的副本。
func (n *Node) replicasFor(key string) []string {
	return n.replicasForRing(n.ring, key)
}

// replicasForRing 返回指定 ring 中负责 key 的副本。
func (n *Node) replicasForRing(ring *hashring.Ring, key string) []string {
	return ring.GetN(key, n.replicas)
}

// Put 分配新版本并执行 quorum 写入。
func (n *Node) Put(ctx context.Context, key string, data []byte) error {
	version, err := n.nextVersion(ctx, key)
	if err != nil {
		return err
	}

	value := store.Value{
		Data:    data,
		Version: version,
	}

	if !n.replicateValue(ctx, key, value) {
		return fmt.Errorf("write quorum not reached")
	}

	return nil
}

// Get 执行 quorum 读取，选择最新版本并修复落后副本。
func (n *Node) Get(ctx context.Context, key string) ([]byte, bool, error) {
	replicas := n.replicasFor(key)
	if len(replicas) == 0 {
		return nil, false, fmt.Errorf("no replicas")
	}

	results := make(chan readResult, len(replicas))

	for _, replica := range replicas {
		go func(replica string) {
			value, found, err := n.getReplica(ctx, replica, key)
			results <- readResult{
				replica: replica,
				value:   value,
				found:   found,
				err:     err,
			}
		}(replica)
	}

	// 从成功响应中选择版本最新的值。
	var latest store.Value
	// hasValue 表示是否存在任意副本值。
	hasValue := false
	success := 0
	successful := make([]readResult, 0, len(replicas))

	for range replicas {
		result := <-results

		if result.err != nil {
			continue
		}

		// 请求本身成功时，无论 key 是否存在都计入 quorum。
		success++
		successful = append(successful, result)

		if !result.found {
			continue
		}

		if !hasValue || newer(result.value, latest) {
			latest = result.value
			hasValue = true
		}
	}

	if success < n.readQuorum {
		return nil, false, fmt.Errorf("read quorum not reached")
	}

	if !hasValue {
		return nil, false, nil
	}

	for _, result := range successful {
		if !result.found || newer(latest, result.value) {
			n.repairReplica(ctx, result.replica, key, latest)
		}
	}

	if latest.Deleted {
		return nil, false, nil
	}

	return latest.Data, true, nil
}

// Delete 为指定 key 分配新版本，并通过 quorum 写入 tombstone。
func (n *Node) Delete(ctx context.Context, key string) error {
	version, err := n.nextVersion(ctx, key)
	if err != nil {
		return err
	}

	value := store.Value{
		Version: version,
		Deleted: true,
	}

	if !n.replicateValue(ctx, key, value) {
		return fmt.Errorf("delete quorum not reached")
	}

	return nil
}
