package chunk

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestStorePutGetDelete 验证 chunk 的写入、读取和删除流程。
func TestStorePutGetDelete(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	want := []byte("chunk data")
	id, err := store.Put(want)
	if err != nil {
		t.Fatal(err)
	}

	exists, err := store.Exists(id)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected chunk to exist")
	}

	got, err := store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("chunk data = %q, want %q", got, want)
	}

	if err := store.Delete(id); err != nil {
		t.Fatal(err)
	}

	exists, err = store.Exists(id)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected chunk not to exist after delete")
	}
}

// TestStoreGetMissing 验证读取不存在的 chunk 会返回 ErrNotFound。
func TestStoreGetMissing(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Get(ID([]byte("missing")))
	if err != ErrNotFound {
		t.Fatalf("get error = %v, want %v", err, ErrNotFound)
	}
}

// TestStorePutSameDataReturnsSameID 验证相同内容得到相同 ID。
func TestStorePutSameDataReturnsSameID(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("same chunk")
	first, err := store.Put(data)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put(data)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("second chunk ID = %q, want %q", second, first)
	}
}

// TestStoreConcurrentPutSameData 验证多个 goroutine 并发写入相同 chunk 不会争用临时文件。
func TestStoreConcurrentPutSameData(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("concurrent chunk data"), 4096)
	wantID := ID(data)

	const writers = 32
	start := make(chan struct{})
	ids := make(chan string, writers)
	errs := make(chan error, writers)

	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			<-start

			id, err := store.Put(data)
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}

	close(start)
	wg.Wait()
	close(ids)
	close(errs)

	for err := range errs {
		t.Errorf("concurrent put failed: %v", err)
	}
	for id := range ids {
		if id != wantID {
			t.Errorf("chunk ID = %q, want %q", id, wantID)
		}
	}

	got, err := store.Get(wantID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("stored chunk data mismatch")
	}

	storedIDs, err := store.IDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(storedIDs) != 1 || storedIDs[0] != wantID {
		t.Fatalf("stored IDs = %v, want [%s]", storedIDs, wantID)
	}
}

// TestStoreGetRejectsTamperedChunk 验证篡改 chunk 文件会失败。
func TestStoreGetRejectsTamperedChunk(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	id, err := store.Put([]byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.path(id), []byte("tampered"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err = store.Get(id)
	if err == nil || err.Error() != "chunk checksum mismatch: "+id {
		t.Fatalf("get error = %v, want checksum mismatch", err)
	}
}

// TestStorePutRepairsTamperedChunk 验证 Put 会覆盖同 ID 的损坏文件。
func TestStorePutRepairsTamperedChunk(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	original := []byte("original")
	id, err := store.Put(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.path(id), []byte("tampered"), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Put(original); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("chunk = %q, want %q", got, original)
	}
}

// TestStoreDeleteIsIdempotent 验证重复删除同一个 chunk 仍成功。
func TestStoreDeleteIsIdempotent(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	id, err := store.Put([]byte("delete me"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(id); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(id); err != nil {
		t.Fatalf("second delete error = %v", err)
	}
}

// TestStoreWriteFromReaderSplitsFinalChunk 验证数据流会保留最后的不完整 chunk。
func TestStoreWriteFromReaderSplitsFinalChunk(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	chunks, err := store.WriteFromReader(strings.NewReader("0123456789"), 4)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"0123", "4567", "89"}
	if len(chunks) != len(want) {
		t.Fatalf("chunk count = %d, want %d", len(chunks), len(want))
	}
	for i, chunk := range chunks {
		data, err := store.Get(chunk.ID)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != want[i] || chunk.Size != len(want[i]) {
			t.Fatalf("chunk %d = %q (%d bytes), want %q (%d bytes)", i, data, chunk.Size, want[i], len(want[i]))
		}
	}
}

// TestStoreWriteFromReaderRejectsInvalidSize 验证非法 chunk 大小会失败。
func TestStoreWriteFromReaderRejectsInvalidSize(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.WriteFromReader(strings.NewReader("data"), 0)
	if err == nil || !strings.Contains(err.Error(), "invalid chunk size") {
		t.Fatalf("write error = %v, want invalid chunk size", err)
	}
}

// TestStoreSweepKeepsLiveChunk 验证 GC 不删除 live 集合中的旧 chunk。
func TestStoreSweepKeepsLiveChunk(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("live")
	id, err := store.Put(data)
	if err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(store.path(id), old, old); err != nil {
		t.Fatal(err)
	}

	result, err := store.Sweep(map[string]struct{}{id: {}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if result.Deleted != 0 {
		t.Fatalf("deleted = %d, want 0", result.Deleted)
	}

	if _, err := store.Get(id); err != nil {
		t.Fatal(err)
	}
}

// TestStoreSweepDeletesOldOrphan 验证 GC 删除过期且未被引用的 chunk。
func TestStoreSweepDeletesOldOrphan(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	id, err := store.Put([]byte("orphan"))
	if err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(store.path(id), old, old); err != nil {
		t.Fatal(err)
	}

	result, err := store.Sweep(nil, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	if result.Deleted != 1 {
		t.Fatalf("deleted = %d, want 1", result.Deleted)
	}

	exists, err := store.Exists(id)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("orphan chunk still exists")
	}
}

// TestStoreSweepKeepsRecentOrphan 验证 GC 保留未到回收时间的孤儿 chunk。
func TestStoreSweepKeepsRecentOrphan(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	id, err := store.Put([]byte("recent"))
	if err != nil {
		t.Fatal(err)
	}

	result, err := store.Sweep(nil, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if result.Deleted != 0 {
		t.Fatalf("deleted = %d, want 0", result.Deleted)
	}

	exists, err := store.Exists(id)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("recent chunk was deleted")
	}
}

// TestStorePutRefreshesExistingChunkModTime 验证复用 chunk 会刷新其回收时间。
func TestStorePutRefreshesExistingChunkModTime(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("reused chunk")
	id, err := store.Put(data)
	if err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-time.Hour)
	path := store.path(id)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Put(data); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if !info.ModTime().After(old) {
		t.Fatalf("mtime was not refreshed: %v", info.ModTime())
	}
}
