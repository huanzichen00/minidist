package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

type pingRequest struct {
	Target string `json:"target"`
}

func (n *Node) pingNode(ctx context.Context, addr string) error {
	if addr == n.addr {
		return nil
	}

	url := "http://" + addr + "/internal/ping"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("node %s returned %s", addr, resp.Status)
	}

	return nil
}

func (n *Node) requestIndirectPing(ctx context.Context, helper string, target string) error {
	payload, err := json.Marshal(pingRequest{
		Target: target,
	})
	if err != nil {
		return err
	}

	url := "http://" + helper + "/internal/ping-request"
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
		return fmt.Errorf("helper %s failed to ping %s: %s", helper, target, resp.Status)
	}

	return nil
}

func (n *Node) indirectHelpers(target string, limit int) []string {
	return n.randomMembers(limit, target)
}

func (n *Node) indirectProbe(ctx context.Context, target string) bool {
	helpers := n.indirectHelpers(target, 2)
	if len(helpers) == 0 {
		return false
	}

	results := make(chan error, len(helpers))

	for _, helper := range helpers {
		go func(helper string) {
			results <- n.requestIndirectPing(ctx, helper, target)
		}(helper)
	}

	for range helpers {
		if err := <-results; err == nil {
			return true
		}
	}

	return false
}

func (n *Node) probeOnce(ctx context.Context) {
	target, ok := n.randomProbeTarget()
	if !ok {
		return
	}

	directCtx, cancelDirect := context.WithTimeout(ctx, 300*time.Millisecond)

	err := n.pingNode(directCtx, target)

	cancelDirect()

	if err == nil {
		n.fd.MarkSuccess(target)
		return
	}

	indirectCtx, cancelIndirect := context.WithTimeout(ctx, 700*time.Millisecond)
	defer cancelIndirect()

	if n.indirectProbe(indirectCtx, target) {
		n.fd.MarkSuccess(target)
		return
	}

	n.fd.MarkFailure(target)
}

func (n *Node) randomProbeTarget() (string, bool) {
	targets := n.randomMembers(1)

	if len(targets) == 0 {
		return "", false
	}

	return targets[0], true
}

func (n *Node) randomMembers(limit int, exclude ...string) []string {
	members := n.fd.List()

	excluded := make(map[string]struct{}, len(exclude)+1)
	excluded[n.addr] = struct{}{}

	for _, node := range exclude {
		excluded[node] = struct{}{}
	}

	candidates := make([]string, 0, len(members))

	for _, member := range members {
		if _, ok := excluded[member]; ok {
			continue
		}

		candidates = append(candidates, member)
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	return candidates
}
