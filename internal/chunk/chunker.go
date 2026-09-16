package chunk

import (
	"fmt"
	"io"
)

// DefaultChunkSize 是默认 chunk 大小。
const DefaultChunkSize = 4 * 1024 * 1024

// ChunkInfo 描述一个已保存的 chunk。
type ChunkInfo struct {
	ID   string `json:"id"`
	Size int    `json:"size"`
}

// WriteFromReader 从数据流中按 chunkSize 拆分并保存数据，返回 chunk 信息。
func (s *Store) WriteFromReader(r io.Reader, chunkSize int) ([]ChunkInfo, error) {
	return WriteChunks(r, chunkSize, s.Put)
}

func WriteChunks(r io.Reader, chunkSize int, put func([]byte) (string, error)) ([]ChunkInfo, error) {
	if r == nil {
		return nil, fmt.Errorf("reader is nil")
	}
	if put == nil {
		return nil, fmt.Errorf("put function is nil")
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	buf := make([]byte, chunkSize)
	var chunks []ChunkInfo

	for {
		n, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}

		id, putErr := put(buf[:n])
		if putErr != nil {
			return nil, putErr
		}

		chunks = append(chunks, ChunkInfo{
			ID:   id,
			Size: n,
		})

		if err == io.ErrUnexpectedEOF {
			break
		}
	}

	return chunks, nil
}
