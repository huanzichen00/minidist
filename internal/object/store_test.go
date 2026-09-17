package object

import (
	"bytes"
	"context"
	"errors"
	"minidist/internal/chunk"
	"testing"
)

type testMetadataStore struct {
	values map[string][]byte
}

type testChunkStore struct {
	store *chunk.Store
}

func (s *testChunkStore) PutChunk(_ context.Context, data []byte) (string, error) {
	return s.store.Put(data)
}

func (s *testChunkStore) GetChunk(_ context.Context, id string) ([]byte, error) {
	return s.store.Get(id)
}

func (s *testMetadataStore) Put(_ context.Context, key string, value []byte) error {
	if s.values == nil {
		s.values = make(map[string][]byte)
	}
	s.values[key] = append([]byte(nil), value...)
	return nil
}

func (s *testMetadataStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	value, ok := s.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *testMetadataStore) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}

// TestStorePutAndWriteTo 验证对象写入后可由 metadata 重建。
func TestStorePutAndWriteTo(t *testing.T) {
	chunks, err := chunk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &testMetadataStore{}
	store := New(&testChunkStore{store: chunks}, metadataStore)

	data := []byte("0123456789")
	ctx := context.Background()
	metadata, err := store.Put(ctx, "file.txt", bytes.NewReader(data), 4)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Name != "file.txt" || metadata.Size != int64(len(data)) || metadata.ChunkSize != 4 {
		t.Fatalf("metadata = %#v", metadata)
	}
	if len(metadata.Chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(metadata.Chunks))
	}
	if _, found, err := metadataStore.Get(ctx, metadataKey("file.txt")); err != nil || !found {
		t.Fatalf("metadata was not persisted: found=%t err=%v", found, err)
	}

	var output bytes.Buffer
	if err := store.WriteTo(ctx, metadata, &output); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), data) {
		t.Fatalf("output = %q, want %q", output.Bytes(), data)
	}
}

// TestStoreWriteToMissingChunk 验证缺失 chunk 会导致对象读取失败。
func TestStoreWriteToMissingChunk(t *testing.T) {
	chunks, err := chunk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := New(&testChunkStore{store: chunks}, &testMetadataStore{})

	metadata, err := store.Put(context.Background(), "file.txt", bytes.NewReader([]byte("data")), 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := chunks.Delete(metadata.Chunks[0].ID); err != nil {
		t.Fatal(err)
	}

	err = store.WriteTo(context.Background(), metadata, &bytes.Buffer{})
	if !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("write error = %v, want %v", err, chunk.ErrNotFound)
	}
}

// TestStorePutRejectsNilReader 验证空 reader 返回错误而不是 panic。
func TestStorePutRejectsNilReader(t *testing.T) {
	chunks, err := chunk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	_, err = New(&testChunkStore{store: chunks}, &testMetadataStore{}).Put(context.Background(), "file.txt", nil, 4)
	if err == nil {
		t.Fatal("expected nil reader error")
	}
}

// TestStoreDeleteRemovesMetadataButKeepsChunks 验证删除对象只删除 metadata，不立即删除共享 chunk。
func TestStoreDeleteRemovesMetadataButKeepsChunks(t *testing.T) {
	chunks, err := chunk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	metadataStore := &testMetadataStore{}
	store := New(&testChunkStore{store: chunks}, metadataStore)
	ctx := context.Background()

	metadata, err := store.Put(ctx, "file.txt", bytes.NewReader([]byte("hello")), 4)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Delete(ctx, "file.txt"); err != nil {
		t.Fatal(err)
	}

	_, found, err := store.Metadata(ctx, "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("metadata still exists after delete")
	}

	for _, info := range metadata.Chunks {
		exists, err := chunks.Exists(info.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("chunk %s was deleted", info.ID)
		}
	}
}

// TestStoreOverwriteCommitsNewMetadata 验证覆盖对象后读取到新内容，旧 chunk 暂时保留。
func TestStoreOverwriteCommitsNewMetadata(t *testing.T) {
	chunks, err := chunk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	store := New(&testChunkStore{store: chunks}, &testMetadataStore{})
	ctx := context.Background()

	oldMetadata, err := store.Put(ctx, "file.txt", bytes.NewReader([]byte("old data")), 4)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Put(ctx, "file.txt", bytes.NewReader([]byte("new data")), 4); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	found, err := store.Get(ctx, "file.txt", &output)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("object not found")
	}
	if output.String() != "new data" {
		t.Fatalf("object = %q, want %q", output.String(), "new data")
	}

	for _, info := range oldMetadata.Chunks {
		exists, err := chunks.Exists(info.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("old chunk %s was deleted", info.ID)
		}
	}
}
