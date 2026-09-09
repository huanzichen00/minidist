package node

import (
	"context"
	"fmt"
	"minidist/internal/store"
)

type versionResult struct {
	counter uint64
	found   bool
	err     error
}

func (n *Node) nextVersion(ctx context.Context, key string) (store.Version, error) {
	replicas := n.replicasFor(key)

	if len(replicas) < n.readQuorum {
		// 版本读取也必须满足 quorum
		return store.Version{}, fmt.Errorf(
			"not enough replicas: got %d, need %d",
			len(replicas),
			n.readQuorum,
		)
	}

	results := make(chan versionResult, len(replicas))

	// 并发读取副本，找出已存在的最大版本号
	for _, replica := range replicas {
		go func(replica string) {
			value, found, err := n.getReplica(ctx, replica, key)
			if err != nil {
				results <- versionResult{
					err: err,
				}
				return
			}
			if !found {
				results <- versionResult{
					found: false,
				}
				return
			}
			results <- versionResult{
				counter: value.Version.Counter,
				found:   true,
			}
		}(replica)
	}

	maxCounter := uint64(0)
	success := 0

	for range replicas {
		result := <-results
		if result.err != nil {
			continue
		}

		// 没有 key 也是成功响应，只是不参与最大版本比较
		success++

		if result.found && result.counter > maxCounter {
			maxCounter = result.counter
		}
	}

	if success < n.readQuorum {
		return store.Version{}, fmt.Errorf(
			"version read quorum not reached: got %d, need %d",
			success,
			n.readQuorum,
		)
	}

	// 同时考虑本节点已分配的版本，避免版本倒退
	for {
		// 取出 atomic.Uint64
		current := n.version.Load()

		base := maxCounter
		if current > base {
			base = current
		}

		next := base + 1

		// CAS 保证并发请求不会分配到相同的本地版本号
		if n.version.CompareAndSwap(current, next) {
			return store.Version{
				Counter: next,
				NodeID:  n.addr,
			}, nil
		}
	}
}
