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
	Version      uint64
}

type memberDebugState struct {
	Node         string    `json:"node"`
	Status       string    `json:"status"`
	LastSuccess  time.Time `json:"last_success"`
	LastFailure  time.Time `json:"last_failure"`
	FailureCount int       `json:"failure_count"`
}

type failureDetector struct {
	mu      sync.RWMutex
	members map[string]memberState
}

type gossipMember struct {
	Node    string `json:"node"`
	Status  string `json:"status"`
	Version uint64 `json:"version"`
}

type gossipRequest struct {
	Members []gossipMember `json:"members"`
}

func newFailureDetector(nodes []string) *failureDetector {
	fd := &failureDetector{
		members: make(map[string]memberState, len(nodes)),
	}
	now := time.Now()
	for _, node := range nodes {
		fd.members[node] = memberState{
			Status:      statusAlive,
			LastSuccess: now,
		}
	}

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
			Node:    node,
			Status:  state.Status.String(),
			Version: state.Version,
		})
	}

	return result
}

func (f *failureDetector) Merge(members []gossipMember) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, remote := range members {
		local, ok := f.members[remote.Node]
		if !ok {
			f.members[remote.Node] = memberState{
				Status:  parseNodeStatus(remote.Status),
				Version: remote.Version,
			}
			continue
		}

		if remote.Version <= local.Version {
			continue
		}

		local.Status = parseNodeStatus(remote.Status)
		local.Version = remote.Version

		f.members[remote.Node] = local
	}
}
