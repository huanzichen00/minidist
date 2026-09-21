package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"minidist/internal/chunk"
	"minidist/internal/object"
	"minidist/internal/store"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
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

// TestHandleGCMark 验证内部 mark 接口返回本地的 live chunk 与配置版本。
func TestHandleGCMark(t *testing.T) {
	node := newReplicationTestNode(t)
	node.configVersion.Store(7)

	metadata, err := json.Marshal(object.Metadata{
		Name:   "file.txt",
		Chunks: []chunk.ChunkInfo{{ID: "chunk-a", Size: 10}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.store.Set(object.MetadataKeyPrefix+"file", store.Value{
		Data: metadata, Version: store.Version{Counter: 1, NodeID: "test"},
	}); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/internal/gc/mark", nil)
	response := httptest.NewRecorder()
	node.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	var result gcMarkResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.ConfigVersion != 7 {
		t.Fatalf("config version = %d, want 7", result.ConfigVersion)
	}
	if !reflect.DeepEqual(result.LiveChunks, []string{"chunk-a"}) {
		t.Fatalf("live chunks = %v, want [chunk-a]", result.LiveChunks)
	}
}

// TestCollectLiveChunks 验证本地与远端 mark 结果会合并为全局集合。
func TestCollectLiveChunks(t *testing.T) {
	node := newReplicationTestNode(t)
	node.configVersion.Store(3)
	node.store.ForceSet(object.MetadataKeyPrefix+"local", store.Value{
		Data: mustMarshalMetadata(t, object.Metadata{
			Name: "local.txt", Chunks: []chunk.ChunkInfo{{ID: "chunk-local", Size: 10}},
		}),
		Version: store.Version{Counter: 1, NodeID: "node-a"},
	})

	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/internal/gc/mark" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(gcMarkResponse{
			ConfigVersion: 3,
			LiveChunks:    []string{"chunk-remote", "chunk-local"},
		})
	}))
	defer remote.Close()

	node.ring.Add(strings.TrimPrefix(remote.URL, "http://"))

	live, version, err := node.collectLiveChunks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if version != 3 {
		t.Fatalf("version = %d, want 3", version)
	}
	if len(live) != 2 {
		t.Fatalf("live chunk count = %d, want 2", len(live))
	}
	for _, id := range []string{"chunk-local", "chunk-remote"} {
		if _, ok := live[id]; !ok {
			t.Fatalf("%s was not collected", id)
		}
	}
}

// TestCollectLiveChunksRejectsConfigVersionMismatch 验证成员配置版本不一致时整轮 mark 失败。
func TestCollectLiveChunksRejectsConfigVersionMismatch(t *testing.T) {
	node := newReplicationTestNode(t)
	node.configVersion.Store(3)

	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gcMarkResponse{
			ConfigVersion: 4,
			LiveChunks:    []string{"chunk-remote"},
		})
	}))
	defer remote.Close()

	node.ring.Add(strings.TrimPrefix(remote.URL, "http://"))

	if _, _, err := node.collectLiveChunks(context.Background()); err == nil {
		t.Fatal("expected config version mismatch error")
	}
}

// TestCollectLiveChunksFailsWhenMemberUnavailable 验证任意成员不可达时不会返回部分 live 集合。
func TestCollectLiveChunksFailsWhenMemberUnavailable(t *testing.T) {
	node := newReplicationTestNode(t)
	node.configVersion.Store(3)
	node.ring.Add("127.0.0.1:1")

	if _, _, err := node.collectLiveChunks(context.Background()); err == nil {
		t.Fatal("expected unavailable member error")
	}
}

// mustMarshalMetadata 将测试 metadata 编码为 JSON。
func mustMarshalMetadata(t *testing.T, metadata object.Metadata) []byte {
	t.Helper()

	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestLocalGCSweepRejectsConfigVersionMismatch 验证版本变化时不会执行回收。
func TestLocalGCSweepRejectsConfigVersionMismatch(t *testing.T) {
	node := newReplicationTestNode(t)
	node.configVersion.Store(2)

	_, err := node.localGCSweep(gcSweepRequest{
		ConfigVersion: 1,
		Cutoff:        time.Now(),
	})
	if err == nil {
		t.Fatal("expected config version mismatch error")
	}
	if !errors.Is(err, errGCConfigVersionChanged) {
		t.Fatalf("error = %v, want config version error", err)
	}
}

// TestHandleGCSweepRejectsConfigVersionMismatch 验证版本冲突映射为 HTTP 409。
func TestHandleGCSweepRejectsConfigVersionMismatch(t *testing.T) {
	node := newReplicationTestNode(t)
	node.configVersion.Store(2)

	body, err := json.Marshal(gcSweepRequest{
		ConfigVersion: 1,
		Cutoff:        time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/internal/gc/sweep", bytes.NewReader(body))
	response := httptest.NewRecorder()

	node.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusConflict)
	}
}

// TestRunGCDeletesOldOrphanAndKeepsLiveChunk 验证 GC 保留引用并删除过期孤儿 chunk。
func TestRunGCDeletesOldOrphanAndKeepsLiveChunk(t *testing.T) {
	node := newReplicationTestNode(t)
	node.configVersion.Store(1)

	liveData := []byte("live chunk")
	liveID, err := node.chunks.Put(liveData)
	if err != nil {
		t.Fatal(err)
	}

	orphanID, err := node.chunks.Put([]byte("orphan chunk"))
	if err != nil {
		t.Fatal(err)
	}

	metadata := object.Metadata{
		Name: "file.txt",
		Chunks: []chunk.ChunkInfo{
			{ID: liveID, Size: len(liveData)},
		},
	}

	node.store.ForceSet(object.MetadataKeyPrefix+"file", store.Value{
		Data:    mustMarshalMetadata(t, metadata),
		Version: store.Version{Counter: 1, NodeID: "test"},
	})

	time.Sleep(20 * time.Millisecond)

	result, err := node.RunGC(context.Background(), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	if result.Deleted != 1 {
		t.Fatalf("deleted = %d, want 1", result.Deleted)
	}

	if _, err := node.chunks.Get(liveID); err != nil {
		t.Fatalf("live chunk missing: %v", err)
	}

	exists, err := node.chunks.Exists(orphanID)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("orphan chunk still exists")
	}
}
