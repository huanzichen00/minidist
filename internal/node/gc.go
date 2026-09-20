package node

import (
	"context"
	"encoding/json"
	"fmt"
	"minidist/internal/object"
	"net/http"
	"sort"
	"strings"
)

type gcMarkResponse struct {
	ConfigVersion uint64   `json:"config_version"`
	LiveChunks    []string `json:"live_chunks"`
}

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

// localGCMark 生成本节点当前配置版本下的 chunk mark 结果。
func (n *Node) localGCMark() (gcMarkResponse, error) {
	live, err := n.markLiveChunks()
	if err != nil {
		return gcMarkResponse{}, err
	}

	chunks := make([]string, 0, len(live))
	for id := range live {
		chunks = append(chunks, id)
	}
	sort.Strings(chunks)

	return gcMarkResponse{
		ConfigVersion: n.configVersion.Load(),
		LiveChunks:    chunks,
	}, nil
}

// requestGCMark 获取指定节点的 chunk mark 结果。
func (n *Node) requestGCMark(ctx context.Context, target string) (gcMarkResponse, error) {
	if target == n.addr {
		return n.localGCMark()
	}

	url := "http://" + target + "/internal/gc/mark"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return gcMarkResponse{}, err
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return gcMarkResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return gcMarkResponse{}, fmt.Errorf("gc mark on %s failed: %s", target, resp.Status)
	}

	var result gcMarkResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return gcMarkResponse{}, err
	}

	return result, nil
}

// collectLiveChunks 收集当前全部成员的 mark 结果，并构造全局 live chunk 集合。
func (n *Node) collectLiveChunks(ctx context.Context) (map[string]struct{}, uint64, error) {
	expectedVersion := n.configVersion.Load()
	members := n.ring.Members()

	if len(members) == 0 {
		return nil, 0, fmt.Errorf("no cluster members")
	}

	live := make(map[string]struct{})

	// 任意节点失败，整轮失败
	for _, member := range members {
		result, err := n.requestGCMark(ctx, member)
		if err != nil {
			return nil, 0, fmt.Errorf("collect gc mark from %s: %w", member, err)
		}

		if result.ConfigVersion != expectedVersion {
			return nil, 0, fmt.Errorf("gc config version mismatch: member=%s expected=%d actual=%d",
				member,
				expectedVersion,
				result.ConfigVersion)
		}

		for _, id := range result.LiveChunks {
			live[id] = struct{}{}
		}
	}

	if n.configVersion.Load() != expectedVersion {
		return nil, 0, fmt.Errorf("cluster config changed during gc mark")
	}

	return live, expectedVersion, nil
}
