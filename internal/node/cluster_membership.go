package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

type addMemberAdminRequest struct {
	Member string `json:"member"`
}

type removeMemberAdminRequest struct {
	Member string `json:"member"`
}

type ClusterConfig struct {
	Version uint64   `json:"version"`
	Members []string `json:"members"`
}

func (n *Node) AddMember(ctx context.Context, member string) error {
	n.configMu.Lock()
	defer n.configMu.Unlock()

	current := n.ring.Members()

	if slices.Contains(current, member) {
		return nil
	}

	allMembers := make([]string, 0, len(current)+1)
	allMembers = append(allMembers, current...)
	allMembers = append(allMembers, member)

	version := n.configVersion.Load() + 1

	config := ClusterConfig{
		Version: version,
		Members: allMembers,
	}

	for _, target := range allMembers {
		if err := n.sendMembershipSync(ctx, target, config); err != nil {
			return err
		}
	}

	for _, target := range allMembers {
		if _, err := n.sendRebalance(ctx, target); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) sendMembershipSync(ctx context.Context, target string, config ClusterConfig) error {
	payload, err := json.Marshal(config)

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

func (n *Node) RemoveMember(ctx context.Context, member string) error {
	n.configMu.Lock()
	defer n.configMu.Unlock()

	current := n.ring.Members()

	if !slices.Contains(current, member) {
		return fmt.Errorf("member %s not found", member)
	}

	if len(current) <= 1 {
		return fmt.Errorf("cannot remove the last member")
	}

	futureMembers := removeMember(current, member)

	// Phase 1:
	// 让即将退出的节点先把数据迁移到 future ring
	if err := n.sendDrain(ctx, member, futureMembers); err != nil {
		return err
	}

	// Phase 2:
	// drain 成功后，才正式更新剩余节点的 membership
	version := n.configVersion.Load() + 1

	config := ClusterConfig{
		Version: version,
		Members: futureMembers,
	}

	for _, target := range futureMembers {
		if err := n.sendMembershipSync(ctx, target, config); err != nil {
			return err
		}
	}

	// Phase 3:
	// 让剩余节点按新的 ring 再做一次 rebalance / cleanup
	for _, target := range futureMembers {
		if _, err := n.sendRebalance(ctx, target); err != nil {
			return err
		}
	}

	return nil
}

func removeMember(members []string, member string) []string {
	result := make([]string, 0, len(members)-1)

	for _, current := range members {
		if current != member {
			result = append(result, current)
		}
	}

	return result
}
