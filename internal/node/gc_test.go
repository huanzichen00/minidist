package node

import (
	"encoding/json"
	"minidist/internal/chunk"
	"minidist/internal/object"
	"minidist/internal/store"
	"testing"
)

// TestMarkLiveChunks 验证只标记未删除对象引用的 chunk。
func TestMarkLiveChunks(t *testing.T) {
	node := newReplicationTestNode(t)

	liveMetadata := object.Metadata{
		Name: "live.txt",
		Chunks: []chunk.ChunkInfo{
			{ID: "chunk-a", Size: 10},
			{ID: "chunk-b", Size: 20},
		},
	}

	data, err := json.Marshal(liveMetadata)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.store.Set(object.MetadataKeyPrefix+"live", store.Value{
		Data:    data,
		Version: store.Version{Counter: 1, NodeID: "test"},
	}); err != nil {
		t.Fatal(err)
	}

	deletedMetadata := object.Metadata{
		Name: "deleted.txt",
		Chunks: []chunk.ChunkInfo{
			{ID: "chunk-deleted", Size: 10},
		},
	}

	deletedData, err := json.Marshal(deletedMetadata)
	if err != nil {
		t.Fatal(err)
	}

	if err := node.store.Set(object.MetadataKeyPrefix+"deleted", store.Value{
		Data:    deletedData,
		Version: store.Version{Counter: 2, NodeID: "test"},
		Deleted: true,
	}); err != nil {
		t.Fatal(err)
	}

	if err := node.store.Set("normal-key", store.Value{
		Data:    []byte("not metadata"),
		Version: store.Version{Counter: 3, NodeID: "test"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := node.store.Set("object:metadata-unrelated", store.Value{
		Data:    []byte("{broken json"),
		Version: store.Version{Counter: 4, NodeID: "test"},
	}); err != nil {
		t.Fatal(err)
	}

	live, err := node.markLiveChunks()
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := live["chunk-a"]; !ok {
		t.Fatal("chunk-a was not marked live")
	}

	if _, ok := live["chunk-b"]; !ok {
		t.Fatal("chunk-b was not marked live")
	}

	if _, ok := live["chunk-deleted"]; ok {
		t.Fatal("deleted object's chunk was marked live")
	}

	if len(live) != 2 {
		t.Fatalf("live chunk count = %d, want 2", len(live))
	}
}

// TestMarkLiveChunksRejectsCorruptedMetadata 验证损坏的对象 metadata 会中止标记。
func TestMarkLiveChunksRejectsCorruptedMetadata(t *testing.T) {
	node := newReplicationTestNode(t)

	if err := node.store.Set(object.MetadataKeyPrefix+"broken", store.Value{
		Data:    []byte("{broken json"),
		Version: store.Version{Counter: 1, NodeID: "test"},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := node.markLiveChunks(); err == nil {
		t.Fatal("expected corrupted metadata error")
	}
}
