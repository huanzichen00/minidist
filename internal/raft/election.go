package raft

// becomeCandidateLocked 进入新的任期并切换为 Candidate
func (r *Raft) becomeCandidateLocked() {
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.id
}

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
