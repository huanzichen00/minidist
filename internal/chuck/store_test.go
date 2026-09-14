package chunk

import (
	"bytes"
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
