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

func newer(a, b store.Value) bool {
	return store.CompareVersion(
		a.Version,
		b.Version,
	) > 0
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
	return n.replicasForRing(n.ring, key)
}

func (n *Node) replicasForRing(ring *hashring.Ring, key string) []string {
	return ring.GetN(key, n.replicas)
}
