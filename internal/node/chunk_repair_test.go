package node

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"minidist/internal/chunk"
)

type safeLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write 追加日志内容。
func (b *safeLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(data)
}

// String 返回当前日志内容。
func (b *safeLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// newChunkRepairTestNode 创建使用指定副本的 chunk repair 测试节点。
func newChunkRepairTestNode(t *testing.T, replicas []string) *Node {
	t.Helper()

	node, err := New("coordinator", replicas, filepath.Join(t.TempDir(), "node.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
	})

	node.replicas = len(replicas)
	return node
}

// TestGetChunkRepairsCorruptedOwner 验证读取健康副本后会修复损坏副本。
func TestGetChunkRepairsCorruptedOwner(t *testing.T) {
	data := []byte("healthy chunk")
	id := chunk.ID(data)
	corrupted := []byte("corrupted chunk")
	repaired := make(chan struct{})

	var mu sync.Mutex
	badData := corrupted
	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write(badData)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			badData = body
			close(repaired)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer badServer.Close()

	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, _ = w.Write(data)
	}))
	defer goodServer.Close()

	node := newChunkRepairTestNode(t, []string{
		strings.TrimPrefix(badServer.URL, "http://"),
		strings.TrimPrefix(goodServer.URL, "http://"),
	})

	got, err := node.GetChunk(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("chunk = %q, want %q", got, data)
	}

	select {
	case <-repaired:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for corrupted replica repair")
	}

	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(badData, data) {
		t.Fatalf("repaired chunk = %q, want %q", badData, data)
	}
}

// TestGetChunkIgnoresUnreachableRepairTarget 验证 repair 失败不会影响成功读取。
func TestGetChunkIgnoresUnreachableRepairTarget(t *testing.T) {
	data := []byte("healthy chunk")
	id := chunk.ID(data)
	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(data)
	}))
	defer goodServer.Close()

	var logs safeLogBuffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
	})

	node := newChunkRepairTestNode(t, []string{
		strings.TrimPrefix(goodServer.URL, "http://"),
		"127.0.0.1:1",
	})

	got, err := node.GetChunk(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("chunk = %q, want %q", got, data)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), "repair chunk") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("repair failure was not logged: %q", logs.String())
}
