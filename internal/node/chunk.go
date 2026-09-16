package node

import (
	"errors"
	"io"
	"minidist/internal/chunk"
	"net/http"
	"strings"
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

	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(data); err != nil {
		return
	}
}
