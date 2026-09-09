package node

import (
	"encoding/json"
	"minidist/internal/store"
	"net/http"
	"strings"
)

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", n.handleKV)
	mux.HandleFunc("/internal/kv/", n.handleInternalKV)
	mux.HandleFunc("/internal/debug/kv/", n.handleDebugKV)
	mux.HandleFunc("/internal/debug/hints", n.handleDebugHints)
	mux.HandleFunc("/internal/ping", n.handlePing)
	mux.HandleFunc("/internal/debug/members", n.handleDebugMembers)
	mux.HandleFunc("/internal/gossip", n.handleGossip)
	mux.HandleFunc("/internal/ping-request", n.handlePingRequest)

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

func (n *Node) handlePing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handlePingRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req pingRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid ping request", http.StatusBadRequest)
		return
	}

	if req.Target == "" {
		http.Error(w, "empty target", http.StatusBadRequest)
		return
	}

	if err := n.pingNode(r.Context(), req.Target); err != nil {
		http.Error(w, "target unreachable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleGossip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req gossipRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid gossip request", http.StatusBadRequest)
		return
	}

	n.fd.Merge(req.Members)

	w.WriteHeader(http.StatusNoContent)
}
