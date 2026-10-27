package raftgrpc

import (
	"context"
	raftv1 "minidist/gen/raft/v1"
	"minidist/internal/raft"

	"google.golang.org/grpc"
)

// Client 用于向其他节点发送 Raft grpc
type Client struct {
	rpc raftv1.RaftServiceClient
}

// NewClient 使用现有 grpc 连接创建 Raft grpc
func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		rpc: raftv1.NewRaftServiceClient(conn),
	}
}

// RequestVote 向远端节点请求投票
func (c *Client) RequestVote(ctx context.Context, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	resp, err := c.rpc.RequestVote(ctx, &raftv1.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}

	return raft.RequestVoteResponse{
		Term:        resp.GetTerm(),
		VoteGranted: resp.GetVoteGranted(),
	}, nil
}
