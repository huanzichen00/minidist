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
	"strings"
	"sync/atomic"
	"time"
)

type Node struct {
	addr string

	ring  *hashring.Ring
	store *store.Memory

	client *http.Client

	// N
	replicas int
	// W
	writeQuorum int

	version atomic.Uint64
}

// goroutine 给 Coordinator 汇报结果 channel 数据结构
type writeResult struct {
	replica string
	err     error
}

type readResult struct {
	replica string
	value   store.Value
	err     error
}

func New(addr string, nodes []string) *Node {
	return &Node{
		addr:  addr,
		ring:  hashring.New(nodes, 100),
		store: store.NewMemory(),

		client: &http.Client{
			Timeout: 2 * time.Second,
		},

		replicas:    3,
		writeQuorum: 2,
	}
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", n.handleKV)
	mux.HandleFunc("/internal/kv/", n.handleInternalKV)

	return mux
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "empty key,", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		n.handleReplicateGet(w, r, key)

	case http.MethodPut:
		n.handleReplicatePut(w, r, key)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleInternalKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "empty key,", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		n.handleInternalPut(w, r, key)
	case http.MethodGet:
		n.handleInternalGet(w, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleInternalGet(w http.ResponseWriter, key string) {
	value, ok := n.store.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, "encode value failed", http.StatusInternalServerError)
		return
	}
}

func (n *Node) handleInternalPut(w http.ResponseWriter, r *http.Request, key string) {
	var value store.Value
	if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
		http.Error(w, "invalid value", http.StatusBadRequest)
		return
	}

	n.store.Set(key, value)
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleReplicateGet(w http.ResponseWriter, r *http.Request, key string) {
	replicas := n.replicasFor(key)

	if len(replicas) == 0 {
		http.Error(w, "no replicas", http.StatusServiceUnavailable)
		return
	}

	value, err := n.getReplica(r.Context(), replicas[0], key, w, r)
	if err != nil {
		http.Error(w, "read replica failed", http.StatusBadGateway)
		return
	}

	_, _ = w.Write(value.Data)
}

func (n *Node) handleReplicatePut(w http.ResponseWriter, r *http.Request, key string) {
	// 先把请求读到内存，防止后续读 EOF
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed:", http.StatusBadRequest)
	}

	value := store.Value{
		Data:    data,
		Version: n.version.Add(1),
	}

	replicas := n.replicasFor(key)

	if len(replicas) == 0 {
		http.Error(w, "no replicas", http.StatusServiceUnavailable)
		return
	}

	if len(replicas) < n.writeQuorum {
		http.Error(w, "not enough replicas", http.StatusServiceUnavailable)
		return
	}

	results := make(chan writeResult, len(replicas))

	for _, replica := range replicas {
		go func(replica string) {
			err := n.putReplica(r.Context(), replica, key, value)
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
			log.Printf("write replica %s failed: %v", result.replica, result.err)
			continue
		}
		success++

		if success >= n.writeQuorum {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	http.Error(w, "write quorum not reached", http.StatusServiceUnavailable)
}

func (n *Node) putReplica(ctx context.Context, replica string, key string, value store.Value) error {
	if replica == n.addr {
		n.store.Set(key, value)
		return nil
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

func (n *Node) forward(owner string, w http.ResponseWriter, r *http.Request) {
	url := fmt.Sprintf("http://%s%s", owner, r.URL.Path)

	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, r.Body)

	if err != nil {
		http.Error(w, "create request failed", http.StatusInternalServerError)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		http.Error(w, "forward request failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)

	_, _ = io.Copy(w, resp.Body)
}

func (n *Node) getReplica(ctx context.Context, replica string, key string, w http.ResponseWriter, r *http.Request) (store.Value, error) {
	if replica == n.addr {
		value, ok := n.store.Get(key)
		if !ok {
			return store.Value{}, fmt.Errorf("key not found")
		}

		return value, nil
	}

	url := fmt.Sprintf("http://%s/internal/kv/%s", replica, key)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		http.Error(w, "create replica failed", http.StatusBadGateway)
		return store.Value{}, err
	}
	resp, err := n.client.Do(req)
	if err != nil {
		http.Error(w, "read replica failed", http.StatusBadGateway)
		return store.Value{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return store.Value{}, fmt.Errorf("key not found")
	}

	if resp.StatusCode > 300 {
		return store.Value{}, fmt.Errorf("replica %s returned %s", replica, resp.Status)
	}
	var value store.Value
	if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
		return store.Value{}, err
	}

	return value, nil
}

func (n *Node) replicasFor(key string) []string {
	return n.ring.GetN(key, n.replicas)
}
