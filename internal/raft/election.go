package raft

// 状态转换
func (r *Raft) becomeCandidateLocked() {
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.id
}

// 搓 rpc 请求
func (r *Raft) requestVoteRequestLocked() RequestVoteRequest {
	lastIndex := uint64(len(r.log) - 1)
	return RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  r.log[lastIndex].Term,
	}
}

// 本地选举入口
func (r *Raft) startElection() RequestVoteRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.becomeCandidateLocked()
	request := r.requestVoteRequestLocked()

	return request
}
