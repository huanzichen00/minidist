package object

import "minidist/internal/chunk"

// Metadata 描述一个逻辑对象由哪些 chunk 组成。
type Metadata struct {
	Name      string            `json:"name"`
	Size      int64             `json:"size"`
	ChunkSize int               `json:"chunk_size"`
	Chunks    []chunk.ChunkInfo `json:"chunks"`
}
