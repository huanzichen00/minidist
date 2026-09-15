package object

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"minidist/internal/chunk"
)

// MetadataStore 定义对象元数据的持久化接口。
type MetadataStore interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// Store 使用 chunk store 保存对象内容。
type Store struct {
	chunks   *chunk.Store
	metadata MetadataStore
}

// New 创建 object store，并复用底层 chunk store。
func New(chunks *chunk.Store, metadata MetadataStore) *Store {
	return &Store{
		chunks:   chunks,
		metadata: metadata,
	}
}

// Put 按 chunkSize 从数据流写入逻辑对象，并生成其元数据。
func (s *Store) Put(ctx context.Context, name string, r io.Reader, chunkSize int) (Metadata, error) {
	if name == "" {
		return Metadata{}, fmt.Errorf("object name is empty")
	}
	if s == nil || s.chunks == nil {
		return Metadata{}, fmt.Errorf("chunk store is nil")
	}
	if s.metadata == nil {
		return Metadata{}, fmt.Errorf("metadata store is nil")
	}
	if r == nil {
		return Metadata{}, fmt.Errorf("object reader is nil")
	}

	chunks, err := s.chunks.WriteFromReader(r, chunkSize)
	if err != nil {
		return Metadata{}, err
	}

	var size int64
	for _, info := range chunks {
		size += int64(info.Size)
	}

	metadata := Metadata{
		Name:      name,
		Size:      size,
		ChunkSize: chunkSize,
		Chunks:    chunks,
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		return Metadata{}, err
	}

	if err := s.metadata.Put(ctx, metadataKey(name), data); err != nil {
		return Metadata{}, err
	}

	return metadata, nil
}

// WriteTo 按 metadata 中的顺序读取 chunk，并写入目标 Writer。
func (s *Store) WriteTo(metadata Metadata, w io.Writer) error {
	if s == nil || s.chunks == nil {
		return fmt.Errorf("chunk store is nil")
	}
	if w == nil {
		return fmt.Errorf("object writer is nil")
	}

	var written int64

	for _, info := range metadata.Chunks {
		data, err := s.chunks.Get(info.ID)
		if err != nil {
			return fmt.Errorf("read chunk %s: %w", info.ID, err)
		}

		if len(data) != info.Size {
			return fmt.Errorf("chunk size mismatch: id=%s metadata=%d actual=%d", info.ID, info.Size, len(data))
		}

		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n != len(data) {
			return io.ErrShortWrite
		}

		written += int64(n)
	}

	if written != metadata.Size {
		return fmt.Errorf("object size mismatch: metadata=%d actual=%d", metadata.Size, written)
	}

	return nil
}

// metadataKey 将对象名转换为稳定的元数据 KV 键。
func metadataKey(name string) string {
	sum := sha256.Sum256([]byte(name))
	return "object:meta:" + hex.EncodeToString(sum[:])
}
