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
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Delete(ctx context.Context, key string) error
}

// Store 使用 chunk store 保存对象内容。
type Store struct {
	chunks   ChunkStore
	metadata MetadataStore
}

// ChunkStore 定义对象层使用的分布式 chunk 读写接口。
type ChunkStore interface {
	PutChunk(ctx context.Context, data []byte) (string, error)
	GetChunk(ctx context.Context, id string) ([]byte, error)
}

// New 创建 object store，并复用底层 chunk store。
func New(chunks ChunkStore, metadata MetadataStore) *Store {
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

	chunks, err := s.writeChunks(ctx, r, chunkSize)
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
func (s *Store) WriteTo(ctx context.Context, metadata Metadata, w io.Writer) error {
	if s == nil || s.chunks == nil {
		return fmt.Errorf("chunk store is nil")
	}
	if w == nil {
		return fmt.Errorf("object writer is nil")
	}

	var written int64

	for _, info := range metadata.Chunks {
		data, err := s.chunks.GetChunk(ctx, info.ID)
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

// Metadata 读取对象元数据。
func (s *Store) Metadata(ctx context.Context, name string) (Metadata, bool, error) {
	if s == nil || s.metadata == nil {
		return Metadata{}, false, fmt.Errorf("metadata store is nil")
	}

	data, found, err := s.metadata.Get(ctx, metadataKey(name))
	if err != nil {
		return Metadata{}, false, err
	}
	if !found {
		return Metadata{}, false, nil
	}

	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return Metadata{}, false, fmt.Errorf("decode metadata: %w", err)
	}

	return metadata, true, nil
}

// Get 读取对象元数据并按顺序写出完整对象。
func (s *Store) Get(ctx context.Context, name string, w io.Writer) (bool, error) {
	metadata, found, err := s.Metadata(ctx, name)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	if err := s.WriteTo(ctx, metadata, w); err != nil {
		return false, err
	}

	return true, nil
}

// writeChunks 从对象数据流中切出 chunk，并通过分布式 chunk store 保存。
func (s *Store) writeChunks(ctx context.Context, r io.Reader, chunkSize int) ([]chunk.ChunkInfo, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	buf := make([]byte, chunkSize)
	var chunks []chunk.ChunkInfo

	for {
		n, readErr := io.ReadFull(r, buf)

		if readErr == io.EOF {
			break
		}

		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return nil, readErr
		}

		id, err := s.chunks.PutChunk(ctx, buf[:n])
		if err != nil {
			return nil, err
		}

		chunks = append(chunks, chunk.ChunkInfo{
			ID:   id,
			Size: n,
		})

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	return chunks, nil
}

// Delete 删除对象的 metadata，但不立即删除其 chunk。
func (s *Store) Delete(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("object name is empty")
	}
	if s == nil || s.metadata == nil {
		return fmt.Errorf("metadata store is nil")
	}

	return s.metadata.Delete(ctx, metadataKey(name))
}
