package node

import (
	"context"
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
