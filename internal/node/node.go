package node

import (
	"fmt"
	"io"
	"minidist/internal/hashring"
	"minidist/internal/store"
	"net/http"
	"strings"
)

type Node struct {
	addr  string
	ring  *hashring.Ring
	store *store.Memory
}

func New(addr string, nodes []string) *Node {
	return &Node{
		addr:  addr,
		ring:  hashring.New(nodes),
		store: store.NewMemory(),
	}
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", n.handleKV)
	return mux
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "empty key,", http.StatusBadRequest)
	}

	owner := n.ring.Get(key)

	if owner == "" {
		http.Error(w, "cluster has no nodes", http.StatusInternalServerError)
		return
	}

	// 将请求转发到负责该 key 的节点
	if owner != n.addr {
		n.forward(owner, w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		n.handleGet(w, key)

	case http.MethodPut:
		n.handlePut(w, r, key)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleGet(w http.ResponseWriter, key string) {
	value, ok := n.store.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Write(value)
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
	}

	n.store.Set(key, value)

	w.WriteHeader(http.StatusNoContent)

}

func (n *Node) forward(owner string, w http.ResponseWriter, r *http.Request) {
	url := fmt.Sprintf("http://%s%s", owner, r.URL.Path)

	req, err := http.NewRequestWithContext(
		r.Context(),
		r.Method,
		url,
		r.Body,
	)

	if err != nil {
		http.Error(w, "create request failed", http.StatusInternalServerError)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "forward request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)

	_, _ = io.Copy(w, resp.Body)
}
