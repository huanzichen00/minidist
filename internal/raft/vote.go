package raft

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

func (r *Raft) RequestVote(req RequestVoteRequest) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 候选节点任期旧，拒绝
	if req.Term < r.currentTerm {
		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}
	}

	// 发现更高任期，更新任期并退回 Follower
	if req.Term > r.currentTerm {
		r.becomeFollowerLocked(req.Term)
	}

	// 一个任期只能投一个候选节点
	// 如果已经投给同一个候选人，允许重复 vote
	if r.votedFor != "" && r.votedFor != req.CandidateID {
		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}
	}

	lastLogIndex := uint64(len(r.log) - 1)
	lastLogTerm := r.log[lastLogIndex].Term

	// Candidate 日志至少和本节点一样新
	if !candidateLogUpToDate(req.LastLogIndex, req.LastLogTerm, lastLogIndex, lastLogTerm) {
		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}
	}

	r.votedFor = req.CandidateID

	return RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: true,
	}
}

func candidateLogUpToDate(candidateIndex uint64, candidateTerm uint64, localIndex uint64, localTerm uint64) bool {
	if candidateTerm != localTerm {
		return candidateTerm > localTerm
	}

	return candidateIndex >= localIndex
}
