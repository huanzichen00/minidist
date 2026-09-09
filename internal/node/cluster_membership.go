package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type membershipUpdateRequest struct {
	Members []string `json:"members"`
}

func (n *Node) AddMember(ctx context.Context, member string) error {
	current := n.fd.List()

	allMembers := make([]string, 0, len(current)+1)
	allMembers = append(allMembers, current...)
	allMembers = append(allMembers, member)

	for _, target := range allMembers {
		if err := n.sendMembershipSync(ctx, target, allMembers); err != nil {
			return err
		}
	}

	for _, target := range allMembers {
		if err := n.sendRebalance(ctx, target); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) sendMembershipSync(ctx context.Context, target string, members []string) error {
	payload, err := json.Marshal(membershipUpdateRequest{
		Members: members,
	})

	if err != nil {
		return err
	}

	url := "http://" + target + "/internal/members/sync"

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
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
		return fmt.Errorf("membership sync to %s failed: %s", target, resp.Status)
	}

	return nil
}
