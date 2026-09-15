package node

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"minidist/internal/store"
)

// newReplicationTestNode 创建单副本的测试节点。
func newReplicationTestNode(t *testing.T) *Node {
	t.Helper()

	node, err := New("node-a", []string{"node-a"}, filepath.Join(t.TempDir(), "node.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})

	node.replicas = 1
	node.readQuorum = 1
	node.writeQuorum = 1
	return node
}

// TestGetReturnsLatestValue 验证 Get 返回 quorum 中的最新值。
func TestGetReturnsLatestValue(t *testing.T) {
	node := newReplicationTestNode(t)
	node.store.ForceSet("foo", store.Value{
		Data:    []byte("value"),
		Version: store.Version{Counter: 1, NodeID: "node-a"},
	})

	data, found, err := node.Get(context.Background(), "foo")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(data) != "value" {
		t.Fatalf("Get = %q, found=%t; want value, true", data, found)
	}
}

// TestGetTreatsTombstoneAsNotFound 验证 tombstone 对外表现为不存在。
func TestGetTreatsTombstoneAsNotFound(t *testing.T) {
	node := newReplicationTestNode(t)
	node.store.ForceSet("foo", store.Value{
		Version: store.Version{Counter: 1, NodeID: "node-a"},
		Deleted: true,
	})

	_, found, err := node.Get(context.Background(), "foo")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected tombstone to be not found")
	}
}

// TestHandleReplicatedGetMapsGetResult 验证 HTTP 层映射 Get 的结果。
func TestHandleReplicatedGetMapsGetResult(t *testing.T) {
	node := newReplicationTestNode(t)
	node.store.ForceSet("foo", store.Value{
		Data:    []byte("value"),
		Version: store.Version{Counter: 1, NodeID: "node-a"},
	})

	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	resp := httptest.NewRecorder()
	node.handleReplicatedGet(resp, req, "foo")

	if resp.Code != http.StatusOK || resp.Body.String() != "value" {
		t.Fatalf("response = %d %q, want 200 value", resp.Code, resp.Body.String())
	}
}
