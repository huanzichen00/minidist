package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

type rebalanceResult struct {
	Scanned int `json:"scanned"`
	Copied  int `json:"copied"`
	Failed  int `json:"failed"`
	Hinted  int `json:"hinted"`
	Cleaned int `json:"cleaned"`

	ChunkScanned int `json:"chunk_scanned"`
	ChunkCopied  int `json:"chunk_copied"`
	ChunkFailed  int `json:"chunk_failed"`
}

// rebalance 按当前 ring 复制副本并清理已失配的本地数据。
func (n *Node) rebalance(ctx context.Context) rebalanceResult {
	snapshot := n.store.Snapshot()

	var result rebalanceResult
	result.Scanned = len(snapshot)

	for key, value := range snapshot {
		replicas := n.replicasFor(key)

		allCopied := true

		for _, replica := range replicas {
			if replica == n.addr {
				continue
			}

			if err := n.putReplica(ctx, replica, key, value); err != nil {
				// 失败就留下一个 hint 稍后处理
				result.Failed++
				n.hints.Add(replica, key, value)
				result.Hinted++
				allCopied = false
				continue
			}

			result.Copied++
		}

		// 当前节点仍然属于 replica set，必须保留本地副本
		if slices.Contains(replicas, n.addr) {
			continue
		}

		// 至少一个新 replica 没有确认成功，保守继续保留旧副本
		if !allCopied {
			continue
		}

		// 只有当前本地版本仍等于 snapshot 中的版本才能删除
		// 防止 rebalance 期间新的写入被误删
		cleaned, err := n.store.DeleteIfMatch(key, value.Version)
		if err != nil {
			result.Failed++
			continue
		}

		if cleaned {
			result.Cleaned++
		}
	}

	result.ChunkScanned, result.ChunkCopied, result.ChunkFailed = n.rebalanceChunks(ctx)

	return result
}

// sendRebalance 请求目标节点执行 rebalance。
func (n *Node) sendRebalance(ctx context.Context, target string) (rebalanceResult, error) {
	url := "http://" + target + "/internal/rebalance"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return rebalanceResult{}, err
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return rebalanceResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return rebalanceResult{}, fmt.Errorf("rebalance on %s failed: %s", target, resp.Status)
	}

	var result rebalanceResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return rebalanceResult{}, err
	}

	return result, nil
}

// rebalanceChunks 按当前 ring 把本地 chunk 复制到当前应该负责它的所有节点
// 当前阶段只复制，不删除旧副本
func (n *Node) rebalanceChunks(ctx context.Context) (scanned int, copied int, failed int) {
	ids, err := n.chunks.IDs()
	if err != nil {
		return 0, 0, 1
	}

	scanned = len(ids)

	for _, id := range ids {
		data, err := n.chunks.Get(id)
		if err != nil {
			failed++
			continue
		}

		replicas := n.replicasFor(id)

		for _, replica := range replicas {
			if replica == n.addr {
				continue
			}

			if err := n.putChunkReplica(ctx, replica, id, data); err != nil {
				failed++
				continue
			}

			copied++
		}
	}

	return scanned, copied, failed
}
