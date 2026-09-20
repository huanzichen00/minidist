package node

import (
	"encoding/json"
	"minidist/internal/chunk"
	"minidist/internal/store"
	"net/http"
	"slices"
	"strings"
)

// Handler 注册节点对外、内部和管理 HTTP 路由。
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
	mux.HandleFunc("/internal/members/sync", n.handleMembershipSync)
	mux.HandleFunc("/internal/rebalance", n.handleRebalance)
	mux.HandleFunc("/internal/gc/mark", n.handleGCMark)
	mux.HandleFunc("/admin/members", n.handleAdminMember)
	mux.HandleFunc("/internal/drain", n.handleDrain)
	mux.HandleFunc("/objects/", n.handleObject)
	mux.HandleFunc("/internal/chunks/", n.handleInternalChunk)

	return mux
}

// handleKV 分发对外 KV 的读写删除请求。
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

// handleInternalKV 分发副本间的内部 KV 请求。
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

// handleInternalGet 返回本地单副本值。
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

// handleInternalPut 写入本地单副本值。
func (n *Node) handleInternalPut(w http.ResponseWriter, r *http.Request, key string) {
	var value store.Value
	if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
		http.Error(w, "invalid value", http.StatusBadRequest)
		return
	}

	if err := n.store.Set(key, value); err != nil {
		http.Error(w, "store value failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handlePing 响应直接健康探测。
func (n *Node) handlePing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handlePingRequest 代替请求方探测目标节点。
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

// handleGossip 合并属于当前 ring 的远端状态。
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

	members := n.ring.Members()

	filtered := make([]gossipMember, 0, len(req.Members))
	for _, member := range req.Members {
		if slices.Contains(members, member.Node) {
			filtered = append(filtered, member)
		}
	}

	n.fd.Merge(filtered)

	w.WriteHeader(http.StatusNoContent)
}

// handleMembershipSync 应用版本更新的完整成员配置。
func (n *Node) handleMembershipSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var config ClusterConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if config.Version == 0 {
		http.Error(w, "invalid config version", http.StatusBadRequest)
		return
	}

	if config.Version <= n.configVersion.Load() {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	current := n.ring.Members()

	for _, member := range current {
		if !slices.Contains(config.Members, member) {
			n.ring.Remove(member)
			n.fd.UntrackMember(member)
		}
	}

	for _, member := range config.Members {
		n.ring.Add(member)
		n.fd.TrackMember(member)
	}

	n.configVersion.Store(config.Version)

	w.WriteHeader(http.StatusNoContent)
}

// handleRebalance 执行本地数据重平衡。
func (n *Node) handleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	result := n.rebalance(r.Context())

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, "encode rebalance result failed", http.StatusInternalServerError)
		return
	}
}

// handleAdminMember 分发管理员成员增删请求。
func (n *Node) handleAdminMember(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		n.handleAdminAddMember(w, r)

	case http.MethodDelete:
		n.handleAdminRemoveMember(w, r)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleAdminAddMember 校验并添加成员。
func (n *Node) handleAdminAddMember(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req addMemberAdminRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Member == "" {
		http.Error(w, "member is required", http.StatusBadRequest)
		return
	}

	if err := n.AddMember(r.Context(), req.Member); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleAdminRemoveMember 校验并移除成员。
func (n *Node) handleAdminRemoveMember(w http.ResponseWriter, r *http.Request) {
	var req removeMemberAdminRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Member == "" {
		http.Error(w, "member is required", http.StatusBadRequest)
		return
	}

	if err := n.RemoveMember(r.Context(), req.Member); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleDrain 按未来成员配置迁移本地数据。
func (n *Node) handleDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req drainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid drain request", http.StatusBadRequest)
		return
	}

	if len(req.FutureMembers) == 0 {
		http.Error(w, "future members cannot be empty", http.StatusBadRequest)
		return
	}

	if err := n.drain(r.Context(), req.FutureMembers); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleObject(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/objects/")
	if name == "" {
		http.Error(w, "empty object name", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		n.handleObjectPut(w, r, name)
	case http.MethodGet:
		n.handleObjectGet(w, r, name)
	case http.MethodDelete:
		n.handleObjectDelete(w, r, name)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleObjectPut(w http.ResponseWriter, r *http.Request, name string) {
	metadata, err := n.objects.Put(r.Context(), name, r.Body, chunk.DefaultChunkSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metadata); err != nil {
		return
	}
}

func (n *Node) handleObjectGet(w http.ResponseWriter, r *http.Request, name string) {
	found, err := n.objects.Get(r.Context(), name, w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !found {
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// handleObjectDelete 删除对象 metadata，chunk 由后续 GC 回收。
func (n *Node) handleObjectDelete(w http.ResponseWriter, r *http.Request, name string) {
	if err := n.objects.Delete(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleGCMark 返回本节点当前可见的 live chunk 集合。
func (n *Node) handleGCMark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	result, err := n.localGCMark()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return
	}
}
