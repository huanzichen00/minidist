package node

import (
	"sync"
	"time"
)

type nodeStatus int

const (
	statusAlive nodeStatus = iota
	statusSuspect
	statusDead
)

type memberState struct {
	Status       nodeStatus
	LastSuccess  time.Time
	LastFailure  time.Time
	FailureCount int

	Incarnation uint64
	Version     uint64
}

type failureDetector struct {
	mu      sync.RWMutex
	self    string
	members map[string]memberState
}

// newFailureDetector 为初始成员创建故障探测器。
func newFailureDetector(nodes []string, self string) *failureDetector {
	fd := &failureDetector{
		self:    self,
		members: make(map[string]memberState, len(nodes)),
	}
	now := time.Now()
	for _, node := range nodes {
		fd.members[node] = memberState{
			Status:      statusAlive,
			LastSuccess: now,
		}
	}

	selfState := fd.members[self]
	selfState.Incarnation = uint64(time.Now().UnixNano())
	selfState.Version = 1

	fd.members[self] = selfState

	return fd
}

// MarkSuccess 记录节点探测成功并恢复 alive 状态。
func (f *failureDetector) MarkSuccess(node string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := f.members[node]

	state.Status = statusAlive
	state.LastSuccess = time.Now()
	state.FailureCount = 0
	state.Version++

	f.members[node] = state
}

// 失败 1~2 次      -> suspect
// 连续失败 >= 3 次 -> Dead
func (f *failureDetector) MarkFailure(node string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	state := f.members[node]

	state.FailureCount++
	state.LastFailure = time.Now()
	state.Version++

	if state.FailureCount >= 3 {
		state.Status = statusDead
	} else {
		state.Status = statusSuspect
	}

	f.members[node] = state
}

// List 返回所有被追踪的节点。
func (f *failureDetector) List() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]string, 0, len(f.members))

	for node := range f.members {
		result = append(result, node)
	}

	return result
}

// String 返回节点状态的字符串表示。
func (s nodeStatus) String() string {
	switch s {
	case statusAlive:
		return "alive"
	case statusSuspect:
		return "suspect"
	case statusDead:
		return "dead"
	default:
		return "unknown"
	}
}

// parseNodeStatus 将字符串解析为节点状态。
func parseNodeStatus(s string) nodeStatus {
	switch s {
	case "alive":
		return statusAlive
	case "suspect":
		return statusSuspect
	case "dead":
		return statusDead
	default:
		return statusSuspect
	}
}

// Snapshot 返回用于调试接口的成员状态快照。
func (f *failureDetector) Snapshot() []memberDebugState {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]memberDebugState, 0, len(f.members))

	for node, state := range f.members {
		result = append(
			result,
			memberDebugState{
				Node:         node,
				Status:       state.Status.String(),
				LastSuccess:  state.LastSuccess,
				LastFailure:  state.LastFailure,
				FailureCount: state.FailureCount,
				Incarnation:  state.Incarnation,
				Version:      state.Version,
			},
		)
	}

	return result
}

// GossipSnapshot 返回用于 gossip 的成员状态快照。
func (f *failureDetector) GossipSnapshot() []gossipMember {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]gossipMember, 0, len(f.members))
	for node, state := range f.members {
		result = append(result, gossipMember{
			Node:        node,
			Status:      state.Status.String(),
			Incarnation: state.Incarnation,
			Version:     state.Version,
		})
	}

	return result
}

// Merge 合并远端 gossip 成员状态。
func (f *failureDetector) Merge(members []gossipMember) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, remote := range members {
		if remote.Node == f.self {
			local := f.members[f.self]
			if parseNodeStatus(remote.Status) != statusAlive &&
				newerMemberState(remote, local) {
				f.refuteSelf(remote)
			}

			continue
		}

		local, ok := f.members[remote.Node]
		if !ok {
			f.members[remote.Node] = memberState{
				Status:      parseNodeStatus(remote.Status),
				Incarnation: remote.Incarnation,
				Version:     remote.Version,
			}
			continue
		}

		if !newerMemberState(remote, local) {
			continue
		}

		local.Status = parseNodeStatus(remote.Status)
		local.Incarnation = remote.Incarnation
		local.Version = remote.Version

		f.members[remote.Node] = local
	}
}

// newerMemberState 判断远端状态是否新于本地状态。
func newerMemberState(remote gossipMember, local memberState) bool {
	if remote.Incarnation > local.Incarnation {
		return true
	}

	if remote.Incarnation < local.Incarnation {
		return false
	}

	return remote.Version > local.Version
}

// refuteSelf 提升自身 incarnation 并重新声明 alive。
func (f *failureDetector) refuteSelf(remote gossipMember) {
	local := f.members[f.self]
	if remote.Incarnation < local.Incarnation {
		return
	}

	local.Incarnation = remote.Incarnation + 1
	local.Version = 1
	local.Status = statusAlive
	local.FailureCount = 0
	local.LastSuccess = time.Now()

	f.members[f.self] = local
}

// TrackMember 开始追踪新成员。
func (f *failureDetector) TrackMember(node string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.members[node]; ok {
		return
	}

	f.members[node] = memberState{
		Status:      statusAlive,
		LastSuccess: time.Now(),
	}
}

// UntrackMember 停止追踪已移除成员。
func (fd *failureDetector) UntrackMember(member string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	delete(fd.members, member)
}
