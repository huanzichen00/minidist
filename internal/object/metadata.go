package object

import "minidist/internal/chunk"

// MetadataKeyPrefix 是对象元数据在 KV store 中的键前缀。
const MetadataKeyPrefix = "object:meta:"

// Metadata 描述一个逻辑对象由哪些 chunk 组成。
type Metadata struct {
	Name      string            `json:"name"`
	Size      int64             `json:"size"`
	ChunkSize int               `json:"chunk_size"`
	Chunks    []chunk.ChunkInfo `json:"chunks"`
}
