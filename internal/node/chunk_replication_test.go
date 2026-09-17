package node

import (
	"context"
	"minidist/internal/chunk"
	"path/filepath"
	"testing"
)

// TestPutChunkWritesLocalReplica 验证本地 chunk 副本不会重复发送 HTTP 请求。
func TestPutChunkWritesLocalReplica(t *testing.T) {
	node, err := New("node-a", []string{"node-a"}, filepath.Join(t.TempDir(), "node.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	node.replicas = 1
	node.writeQuorum = 1

	id, err := node.PutChunk(context.Background(), []byte("chunk"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.chunks.Get(id); err != nil {
		t.Fatal(err)
	}
}

// TestRepairChunkRestoresMissingLocalReplica 验证 repair 会恢复缺失的本地副本。
func TestRepairChunkRestoresMissingLocalReplica(t *testing.T) {
	node, err := New("node-a", []string{"node-a"}, filepath.Join(t.TempDir(), "node.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	data := []byte("repair me")
	id := chunk.ID(data)
	node.repairChunk(id, data, "remote-node", []string{"remote-node", "node-a"})

	got, err := node.chunks.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("chunk = %q, want %q", got, data)
	}
}
