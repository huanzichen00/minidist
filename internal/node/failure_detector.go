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

type memberDebugState struct {
	Node         string    `json:"node"`
	Status       string    `json:"status"`
	LastSuccess  time.Time `json:"last_success"`
	LastFailure  time.Time `json:"last_failure"`
	FailureCount int       `json:"failure_count"`

	Incarnation uint64 `json:"incarnation"`
	Version     uint64 `json:"version"`
}

type failureDetector struct {
	mu      sync.RWMutex
	self    string
	members map[string]memberState
}

type gossipMember struct {
	Node        string `json:"node"`
	Status      string `json:"status"`
	Version     uint64 `json:"version"`
	Incarnation uint64 `json:"incarnation"`
}

type gossipRequest struct {
	Members []gossipMember `json:"members"`
}

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

func (f *failureDetector) List() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := make([]string, 0, len(f.members))

	for node := range f.members {
		result = append(result, node)
	}

	return result
}

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

func newerMemberState(remote gossipMember, local memberState) bool {
	if remote.Incarnation > local.Incarnation {
		return true
	}

	if remote.Incarnation < local.Incarnation {
		return false
	}

	return remote.Version > local.Version
}

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
