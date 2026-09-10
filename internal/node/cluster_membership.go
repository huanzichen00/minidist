package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
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

	previous := ClusterConfig{
		Version: n.configVersion.Load(),
		Members: current,
	}
	next := ClusterConfig{
		Version: previous.Version + 1,
		Members: allMembers,
	}

	return n.applyClusterConfig(ctx, allMembers, previous, next)
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

	previous := ClusterConfig{
		Version: n.configVersion.Load(),
		Members: current,
	}
	next := ClusterConfig{
		Version: previous.Version + 1,
		Members: futureMembers,
	}

	return n.applyClusterConfig(ctx, futureMembers, previous, next)
}

func (n *Node) applyClusterConfig(
	ctx context.Context,
	targets []string,
	previous ClusterConfig,
	next ClusterConfig,
) error {
	applied := make([]string, 0, len(targets))

	for _, target := range targets {
		if err := n.sendMembershipSync(ctx, target, next); err != nil {
			n.compensateClusterConfig(ctx, applied, previous, next.Version+1)
			return fmt.Errorf("sync config to %s: %w", target, err)
		}

		applied = append(applied, target)
	}

	for _, target := range targets {
		if _, err := n.sendRebalance(ctx, target); err != nil {
			n.compensateClusterConfig(ctx, applied, previous, next.Version+1)
			return fmt.Errorf("rebalance on %s: %w", target, err)
		}
	}

	return nil
}

func (n *Node) compensateClusterConfig(
	ctx context.Context,
	targets []string,
	previous ClusterConfig,
	version uint64,
) {
	rollback := ClusterConfig{
		Version: version,
		Members: previous.Members,
	}

	for _, target := range targets {
		if err := n.sendMembershipSync(ctx, target, rollback); err != nil {
			log.Printf("compensate config on %s failed: %v", target, err)
		}
	}

	if !slices.Contains(targets, n.addr) {
		n.configVersion.Store(version)
	}
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
