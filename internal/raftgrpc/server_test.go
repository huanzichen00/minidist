package raftgrpc

import (
	"context"
	"net"
	"testing"
	"time"

	raftv1 "minidist/gen/raft/v1"
	"minidist/internal/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestServerRequestVote 通过内存 gRPC 连接验证 server 转发请求和返回结果。
func TestServerRequestVote(t *testing.T) {
	r := raft.New("node-a", nil, make(chan raft.ApplyMsg, 8))
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = listener.Close() })

	server := grpc.NewServer()
	raftv1.RegisterRaftServiceServer(server, NewServer(r))
	t.Cleanup(server.Stop)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve gRPC: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///raft-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := raftv1.NewRaftServiceClient(conn)

	// 按顺序复用同一个节点，验证投票记录和任期会在 RPC 之间保留。
	tests := []struct {
		name        string
		term        uint64
		candidateID string
		wantTerm    uint64
		wantGranted bool
	}{
		{"first vote", 3, "node-b", 3, true},
		{"same candidate retries", 3, "node-b", 3, true},
		{"another candidate in same term", 3, "node-c", 3, false},
		{"stale term", 2, "node-b", 3, false},
		{"higher term resets vote", 4, "node-c", 4, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err := client.RequestVote(ctx, &raftv1.RequestVoteRequest{
				Term:        tt.term,
				CandidateId: tt.candidateID,
			})
			if err != nil {
				t.Fatalf("RequestVote: %v", err)
			}
			if resp.GetTerm() != tt.wantTerm || resp.GetVoteGranted() != tt.wantGranted {
				t.Fatalf("response = (term=%d, granted=%t), want (term=%d, granted=%t)",
					resp.GetTerm(), resp.GetVoteGranted(), tt.wantTerm, tt.wantGranted)
			}
			if got := r.CurrentTerm(); got != tt.wantTerm {
				t.Fatalf("Raft term = %d, want %d", got, tt.wantTerm)
			}
		})
	}
}
