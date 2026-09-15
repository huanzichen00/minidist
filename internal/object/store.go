package object

import (
	"fmt"
	"io"
	"minidist/internal/chunk"
)

type Store struct {
	chunks *chunk.Store
}

// New 创建 object store，并复用底层 chunk store。
func New(chunks *chunk.Store) *Store {
	return &Store{
		chunks: chunks,
	}
}

// Put 从数据流写入一个逻辑对象，并生成它的 metadata。
func (s *Store) Put(name string, r io.Reader, chunkSize int) (Metadata, error) {
	if name == "" {
		return Metadata{}, fmt.Errorf("object name is empty")
	}
	if s == nil || s.chunks == nil {
		return Metadata{}, fmt.Errorf("chunk store is nil")
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

	return Metadata{
		Name:      name,
		Size:      size,
		ChunkSize: chunkSize,
		Chunks:    chunks,
	}, nil
}

// WriteTo 根据 metadata 按顺序读取 chunk，并写入目标 Writer。
func (s *Store) WriteTo(metadata Metadata, w io.Writer) error {
	if s == nil || s.chunks == nil {
		return fmt.Errorf("chunk store is nil")
	}
	if w == nil {
		return fmt.Errorf("object writer is nil")
	}

	for _, info := range metadata.Chunks {
		data, err := s.chunks.Get(info.ID)
		if err != nil {
			return err
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
	}

	return nil
}
