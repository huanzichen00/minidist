package node

import (
	"context"
	"fmt"
	"minidist/internal/store"
	"sync"
)

type hint struct {
	Replica string      `json:"replica"`
	Key     string      `json:"key"`
	Value   store.Value `json:"value"`
}

type hintStore struct {
	mu    sync.Mutex
	hints map[string]hint
}

func newHintStore() *hintStore {
	return &hintStore{
		hints: make(map[string]hint),
	}
}

func hintKey(replica string, key string) string {
	return fmt.Sprintf("%s|%s", replica, key)
}

func (h *hintStore) Add(replica string, key string, value store.Value) {
	h.mu.Lock()
	defer h.mu.Unlock()

	k := hintKey(replica, key)

	current, ok := h.hints[k]
	if ok {
		// 比 hints 里的版本还小，直接返回
		if !newer(value, current.Value) {
			return
		}
	}

	h.hints[k] = hint{
		Replica: replica,
		Key:     key,
		Value:   value,
	}
}

func (h *hintStore) List() []hint {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([]hint, 0, len(h.hints))
	for _, hint := range h.hints {
		result = append(result, hint)
	}

	return result
}

func (h *hintStore) RemoveIfMatch(replica string, key string, version store.Version) {
	h.mu.Lock()
	defer h.mu.Unlock()

	k := hintKey(replica, key)
	current, ok := h.hints[k]
	if !ok {
		return
	}
	// 只有当前 Hint 仍然是刚刚成功 handoff 的那个版本，才允许删除
	if store.CompareVersion(current.Value.Version, version) != 0 {
		return
	}
	delete(h.hints, k)
}

func (n *Node) flushHints(ctx context.Context) {
	hints := n.hints.List()

	for _, hint := range hints {
		err := n.putReplica(ctx, hint.Replica, hint.Key, hint.Value)
		if err != nil {
			continue
		}

		n.hints.RemoveIfMatch(hint.Replica, hint.Key, hint.Value.Version)
	}
}
