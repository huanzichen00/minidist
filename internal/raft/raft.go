package raft

import "sync"

// Role 表示 Raft 节点当前角色
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// LogEntry 是 Raft 日志中的一条记录
// Index 不单独存储, entry 在 log slice 中的位置就是它的日志索引
type LogEntry struct {
	Term    uint64
	Command []byte
}

// ApplyMsg 表示已经 committed, 可以交给上层状态机执行的日志
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Raft 保存单个 Raft 节点的核心状态
type Raft struct {
	mu sync.Mutex

	id    string
	peers []string

	role        Role
	currentTerm uint64
	votedFor    string

	// log[0] 是固定的 dummy entry
	// 因此真实日志从 index=1 开始
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64

	applyCh chan<- ApplyMsg
}

// New 创建一个初始状态为 Follower 的 Raft 节点
func New(id string, peers []string, applyCh chan<- ApplyMsg) *Raft {
	return &Raft{
		id:      id,
		peers:   append([]string(nil), peers...),
		role:    Follower,
		log:     []LogEntry{{}},
		applyCh: applyCh,
	}
}

// CurrentTerm 返回当前 term
func (r *Raft) CurrentTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.currentTerm
}

// Role 返回当前节点角色
func (r *Raft) Role() Role {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.role
}

// LastLogIndex 返回当前最后一条日志的 index
func (r *Raft) LastLogIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return uint64(len(r.log) - 1)
}

// CommitIndex 返回当前已提交到的最大日志 index
func (r *Raft) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.commitIndex
}

// LastApplied 返回已经交给状态机执行到的最大日志 index
func (r *Raft) LastApplied() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lastApplied
}

// becomeFollower 将指定节点切换为指定 term 下的 Follower
func (r *Raft) becomeFollower(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// term 小于现任期，视为无效返回
	if term < r.currentTerm {
		return
	}

	// 更新任期
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
	}

	r.role = Follower
}

// appendEntry 向本地日志尾部追加一条记录, 并返回新日志的 index
func (r *Raft) appendEntry(term uint64, command []byte) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry := LogEntry{
		Term:    term,
		Command: append([]byte(nil), command...),
	}

	r.log = append(r.log, entry)
	return uint64(len(r.log) - 1)
}

// termAt 返回指定日志 index 对应的 term
func (r *Raft) termAt(index uint64) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index >= uint64(len(r.log)) {
		return 0, false
	}

	return r.log[index].Term, true
}

// truncateFrom 删除 index 及其之后的日志
// Raft follower 收到冲突 AppendEntries 时会使用这个操作
func (r *Raft) truncateFrom(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.log = r.log[:index]
}

// advanceCommit 将 commitIndex 推进到指定位置, 并返回需要 apply 的日志
func (r *Raft) advanceCommit() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldCommitIndex := r.commitIndex
	r.commitIndex = uint64(len(r.log)) - 1

	if r.commitIndex == oldCommitIndex {
		return []LogEntry{}
	}
	return r.log[oldCommitIndex+1:]
}

// applyCommitted 将新提交的日志发送给上层状态机
