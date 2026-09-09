package node

import (
	"encoding/json"
	"minidist/internal/store"
	"net/http"
	"strings"
	"time"
)

type memberDebugState struct {
	Node         string    `json:"node"`
	Status       string    `json:"status"`
	LastSuccess  time.Time `json:"last_success"`
	LastFailure  time.Time `json:"last_failure"`
	FailureCount int       `json:"failure_count"`

	Incarnation uint64 `json:"incarnation"`
	Version     uint64 `json:"version"`
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

func (n *Node) handleDebugMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(n.fd.Snapshot()); err != nil {
		http.Error(w, "encode members failed", http.StatusInternalServerError)
		return
	}
}
