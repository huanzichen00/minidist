package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type rebalanceResult struct {
	Scanned int `json:"scanned"`
	Copied  int `json:"copied"`
	Failed  int `json:"failed"`
	Hinted  int `json:"hinted"`
}

func (n *Node) rebalance(ctx context.Context) rebalanceResult {
	snapshot := n.store.Snapshot()

	var result rebalanceResult
	result.Scanned = len(snapshot)

	for key, value := range snapshot {
		replicas := n.replicasFor(key)
		for _, replica := range replicas {
			if replica == n.addr {
				continue
			}

			if err := n.putReplica(ctx, replica, key, value); err != nil {
				// 失败就留下一个 hint 稍后处理
				result.Failed++
				n.hints.Add(replica, key, value)
				result.Hinted++
				continue
			}

			result.Copied++
		}
	}

	return result
}

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
