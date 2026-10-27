package raftgrpc

import (
	"context"
	raftv1 "minidist/gen/raft/v1"
	"minidist/internal/raft"
)

// Server 将 grpc 请求转发给 Raft 核心
type Server struct {
	raftv1.UnimplementedRaftServiceServer

	raft *raft.Raft
}

// NewServer 创建 Raft 的 grpc 适配器
func NewServer(r *raft.Raft) *Server {
	return &Server{raft: r}
}

func (s *Server) RequestVote(_ context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	result := s.raft.RequestVote(raft.RequestVoteRequest{
		Term:         req.GetTerm(),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	})

	return &raftv1.RequestVoteResponse{
		Term:        result.Term,
		VoteGranted: result.VoteGranted,
	}, nil
}
