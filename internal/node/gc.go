package node

import (
	"encoding/json"
	"fmt"
	"minidist/internal/object"
	"strings"
)

// markLiveChunks 扫描本节点当前可见的 object metadata，返回其中引用的所有 chunk ID。
func (n *Node) markLiveChunks() (map[string]struct{}, error) {
	snapshot := n.store.Snapshot()
	live := make(map[string]struct{})

	for key, value := range snapshot {
		if !strings.HasPrefix(key, object.MetadataKeyPrefix) {
			continue
		}

		if value.Deleted {
			continue
		}

		var metadata object.Metadata
		if err := json.Unmarshal(value.Data, &metadata); err != nil {
			return nil, fmt.Errorf("decode object metadata: %s: %w", key, err)
		}

		for _, info := range metadata.Chunks {
			live[info.ID] = struct{}{}
		}
	}

	return live, nil
}
