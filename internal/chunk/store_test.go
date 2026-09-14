package chunk

import (
	"bytes"
	"os"
	"testing"
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
