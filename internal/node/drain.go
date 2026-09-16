package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"minidist/internal/hashring"
	"net/http"
)

type drainRequest struct {
	FutureMembers []string `json:"future_members"`
}

// drain 将本地 KV 和 chunk 数据复制到未来成员配置的副本节点。
func (n *Node) drain(ctx context.Context, futureMembers []string) error {
	futureRing := hashring.New(futureMembers, n.virtualNodes)

	// 先迁移 KV 数据
	snapshot := n.store.Snapshot()

	for key, value := range snapshot {
		replicas := n.replicasForRing(futureRing, key)

		for _, replica := range replicas {
			if err := n.putReplica(ctx, replica, key, value); err != nil {
				return fmt.Errorf("drain: copy key %q to %s: %w", key, replica, err)
			}
		}
	}

	// 再迁移本节点保存的 chunk
	ids, err := n.chunks.IDs()
	if err != nil {
		return fmt.Errorf("drain: list chunks: %w", err)
	}

	for _, id := range ids {
		data, err := n.chunks.Get(id)
		if err != nil {
			return fmt.Errorf("drain: read chunks %s: %w", id, err)
		}

		replicas := n.replicasForRing(futureRing, id)

		for _, replica := range replicas {
			if err := n.putChunkReplica(ctx, replica, id, data); err != nil {
				return fmt.Errorf("drain: copy chunk %s to %s: %w", id, replica, err)
			}
		}
	}

	return nil
}

// sendDrain 请求待移除节点向未来成员配置迁移数据。
func (n *Node) sendDrain(ctx context.Context, target string, futureMembers []string) error {
	payload, err := json.Marshal(drainRequest{
		FutureMembers: futureMembers,
	})
	if err != nil {
		return err
	}

	url := "http://" + target + "/internal/drain"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("drain %s failed: %s", target, resp.Status)
	}

	return nil
}
