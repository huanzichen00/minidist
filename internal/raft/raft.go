package raft

import (
	"fmt"
	"sync"
)

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
func (r *Raft) becomeFollowerLocked(term uint64) {

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
// 调用者必须持有 r.mu
func (r *Raft) appendEntryLocked(term uint64, command []byte) uint64 {
	entry := LogEntry{
		Term:    term,
		Command: append([]byte(nil), command...),
	}

	r.log = append(r.log, entry)
	return uint64(len(r.log) - 1)
}

// appendEntry 向本地日志尾部追加一条记录, 并返回新日志的 index
func (r *Raft) appendEntry(term uint64, command []byte) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.appendEntryLocked(term, command)
}

// termAt 返回指定日志 index 对应的 term
// 调用者必须持有 r.mu
func (r *Raft) termAtLocked(index uint64) (uint64, bool) {
	if index >= uint64(len(r.log)) {
		return 0, false
	}

	return r.log[index].Term, true
}

// truncateFrom 删除 index 及其之后的日志
// 调用者必须持有 r.mu
func (r *Raft) truncateFromLocked(index uint64) error {

	if index == 0 {
		return fmt.Errorf("cannot truncate dummy entry")
	}

	if index >= uint64(len(r.log)) {
		return fmt.Errorf("truncate index out of range: %d", index)
	}

	if index <= r.commitIndex {
		return fmt.Errorf("cannot truncate committed log: index=%d commit=%d", index, r.commitIndex)
	}

	r.log = r.log[:index]
	return nil
}

// advanceCommit 将 commitIndex 推进到指定位置
// 调用者必须持有 r.mu
func (r *Raft) advanceCommitLocked(index uint64) error {

	lastIndex := uint64(len(r.log) - 1)

	if index > lastIndex {
		return fmt.Errorf("commit index out of range: %d > %d", index, lastIndex)
	}

	if index <= r.commitIndex {
		return nil
	}

	r.commitIndex = index
	return nil
}

func (r *Raft) advanceCommit(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.advanceCommitLocked(index)
}

// applyCommitted 将尚未 apply 的 committed 日志按顺序发送给状态机
// 当前假设只有一个 applier 调用该函数
func (r *Raft) applyCommitted() {
	for {
		r.mu.Lock()

		if r.lastApplied >= r.commitIndex {
			r.mu.Unlock()
			return
		}

		index := r.lastApplied + 1
		entry := r.log[index]

		msg := ApplyMsg{
			Index:   index,
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		}

		r.mu.Unlock()

		r.applyCh <- msg

		r.mu.Lock()
		r.lastApplied = index
		r.mu.Unlock()
	}
}

// termAt 返回指定日志 index 对应的 term。
func (r *Raft) termAt(index uint64) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.termAtLocked(index)
}

// truncateFrom 删除 index 及其之后的日志。
func (r *Raft) truncateFrom(index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.truncateFromLocked(index)
}
