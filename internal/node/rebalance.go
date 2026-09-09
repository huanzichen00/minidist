package node

import (
	"context"
	"fmt"
	"net/http"
)

func (n *Node) rebalance(ctx context.Context) {
	snapshot := n.store.Snapshot()

	for key, value := range snapshot {
		replicas := n.replicasFor(key)
		for _, replica := range replicas {
			if replica == n.addr {
				continue
			}

			if err := n.putReplica(ctx, replica, key, value); err != nil {
				// 失败就留下一个 hint 稍后处理
				n.hints.Add(replica, key, value)
			}
		}
	}
}

func (n *Node) sendRebalance(ctx context.Context, target string) error {
	url := "http://" + target + "/internal/rebalance"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("rebalance on %s failed: %s", target, resp.Status)
	}

	return nil
}
