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
