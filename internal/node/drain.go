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

func (n *Node) drain(ctx context.Context, futureMembers []string) error {
	futureRing := hashring.New(futureMembers, n.virtualNodes)

	snapshot := n.store.Snapshot()

	for key, value := range snapshot {
		replicas := n.replicasForRing(futureRing, key)

		for _, replica := range replicas {
			if err := n.putReplica(ctx, replica, key, value); err != nil {
				return fmt.Errorf("drain: copy key %q to %s: %w", key, replica, err)
			}
		}
	}

	return nil
}

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
