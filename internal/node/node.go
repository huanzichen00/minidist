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
	// R
	readQuorum int

	version atomic.Uint64

	hints *hintStore
}

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

type versionResult struct {
	counter uint64
	found   bool
	err     error
}

func newer(a, b store.Value) bool {
	return store.CompareVersion(
		a.Version,
		b.Version,
	) > 0
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
		readQuorum:  2,

		hints: newHintStore(),
	}
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", n.handleKV)
	mux.HandleFunc("/internal/kv/", n.handleInternalKV)
	mux.HandleFunc("/internal/debug/kv/", n.handleDebugKV)
	mux.HandleFunc("/internal/debug/hints", n.handleDebugHints)

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
		n.handleReplicatedGet(w, r, key)

	case http.MethodPut:
		n.handleReplicatedPut(w, r, key)

	case http.MethodDelete:
		n.handleReplicatedDelete(w, r, key)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleInternalKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/kv/")
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

func (n *Node) handleReplicatedGet(w http.ResponseWriter, r *http.Request, key string) {
	replicas := n.replicasFor(key)

	if len(replicas) == 0 {
		http.Error(w, "no replicas", http.StatusServiceUnavailable)
		return
	}

	results := make(chan readResult, len(replicas))
	for _, replica := range replicas {
		go func(replica string) {
			value, found, err := n.getReplica(r.Context(), replica, key)
			results <- readResult{
				replica: replica,
				value:   value,
				found:   found,
				err:     err,
			}
		}(replica)
	}

	// 现在得到的成功结果里，版本最新的那个
	var latest store.Value
	// latest 是否有实际内容，防止传回空值
	hasValue := false
	success := 0
	successful := make([]readResult, 0, len(replicas))

	for range replicas {
		result := <-results

		if result.err != nil {
			continue
		}

		// 请求本身成功，不管有木有 key，
		// 都算 replica 请求成功
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
		http.Error(w, "read quorum not reached", http.StatusServiceUnavailable)
		return
	}

	if !hasValue {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	for _, result := range successful {
		if !result.found {
			n.repairReplica(r.Context(), result.replica, key, latest)
			continue
		}

		if newer(latest, result.value) {
			n.repairReplica(r.Context(), result.replica, key, latest)
		}
	}

	if latest.Deleted {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	_, _ = w.Write(latest.Data)
}

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

func (n *Node) handleReplicatedPut(w http.ResponseWriter, r *http.Request, key string) {
	// 先把请求读到内存，防止后续读 EOF
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed:", http.StatusBadRequest)
		return
	}

	version, err := n.nextVersion(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	value := store.Value{Data: data, Version: version}

	if !n.replicateValue(r.Context(), key, value) {
		http.Error(w, "write quorum not reached", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleReplicatedDelete(w http.ResponseWriter, r *http.Request, key string) {
	version, err := n.nextVersion(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	value := store.Value{
		Version: version,
		Deleted: true,
	}

	if !n.replicateValue(r.Context(), key, value) {
		http.Error(w, "delete quorum not reached", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleDebugKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/debug/kv/")
	if key == "" {
		http.Error(w, "empty key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		n.handleDebugGet(w, key)
	case http.MethodPut:
		n.handleDebugPut(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleDebugGet(w http.ResponseWriter, key string) {
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

func (n *Node) handleDebugPut(w http.ResponseWriter, r *http.Request, key string) {
	var value store.Value
	if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
		http.Error(w, "invalid value", http.StatusBadRequest)
		return
	}

	n.store.ForceSet(key, value)

	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleDebugHints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hints := n.hints.List()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(hints); err != nil {
		http.Error(w, "encode hints failed", http.StatusInternalServerError)
		return
	}
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

func (n *Node) repairReplica(ctx context.Context, replica string, key string, value store.Value) {
	err := n.putReplica(ctx, replica, key, value)
	if err != nil {
		log.Printf("repair replica %s failed: %v", replica, err)
	}
}

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

func (n *Node) replicasFor(key string) []string {
	return n.ring.GetN(key, n.replicas)
}

func (n *Node) nextVersion(ctx context.Context, key string) (store.Version, error) {
	replicas := n.replicasFor(key)

	if len(replicas) < n.readQuorum {
		// 版本读取也必须满足 quorum
		return store.Version{}, fmt.Errorf(
			"not enough replicas: got %d, need %d",
			len(replicas),
			n.readQuorum,
		)
	}

	results := make(chan versionResult, len(replicas))

	// 并发读取副本，找出已存在的最大版本号
	for _, replica := range replicas {
		go func(replica string) {
			value, found, err := n.getReplica(ctx, replica, key)
			if err != nil {
				results <- versionResult{
					err: err,
				}
				return
			}
			if !found {
				results <- versionResult{
					found: false,
				}
				return
			}
			results <- versionResult{
				counter: value.Version.Counter,
				found:   true,
			}
		}(replica)
	}

	maxCounter := uint64(0)
	success := 0

	for range replicas {
		result := <-results
		if result.err != nil {
			continue
		}

		// 没有 key 也是成功响应，只是不参与最大版本比较
		success++

		if result.found && result.counter > maxCounter {
			maxCounter = result.counter
		}
	}

	if success < n.readQuorum {
		return store.Version{}, fmt.Errorf(
			"version read quorum not reached: got %d, need %d",
			success,
			n.readQuorum,
		)
	}

	// 同时考虑本节点已分配的版本，避免版本倒退
	for {
		// 取出 atomic.Uint64
		current := n.version.Load()

		base := maxCounter
		if current > base {
			base = current
		}

		next := base + 1

		// CAS 保证并发请求不会分配到相同的本地版本号
		if n.version.CompareAndSwap(current, next) {
			return store.Version{
				Counter: next,
				NodeID:  n.addr,
			}, nil
		}
	}
}

func (n *Node) runHintLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.flushHints(ctx)

		case <-ctx.Done():
			return
		}
	}
}

func (n *Node) RunBackground(ctx context.Context) {
	go n.runHintLoop(ctx)
}
