package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type gossipMember struct {
	Node        string `json:"node"`
	Status      string `json:"status"`
	Version     uint64 `json:"version"`
	Incarnation uint64 `json:"incarnation"`
}

type gossipRequest struct {
	Members []gossipMember `json:"members"`
}

func (n *Node) sendGossip(ctx context.Context, target string) error {
	members := n.fd.GossipSnapshot()

	payload, err := json.Marshal(gossipRequest{
		Members: members,
	})

	if err != nil {
		return err
	}

	url := "http://" + target + "/internal/gossip"

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
		return fmt.Errorf("gossip target %s returned %s", target, resp.Status)
	}

	return nil
}

func (n *Node) gossipTargets(limit int) []string {
	return n.randomMembers(limit)
}

func (n *Node) gossipOnce(ctx context.Context) {
	for _, target := range n.gossipTargets(2) {
		if err := n.sendGossip(ctx, target); err != nil {
			continue
		}
	}
}
