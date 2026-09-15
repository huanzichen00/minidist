package node

import (
	"context"
	"log"
	"time"
)

type backgroundTask struct {
	name     string
	interval time.Duration
	run      func(context.Context)
}

// runPeriodicTask 按固定间隔运行任务并隔离任务 panic。
func (n *Node) runPeriodicTask(
	ctx context.Context,
	name string,
	interval time.Duration,
	task func(context.Context),
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("background task %s panic: %v", name, r)
					}
				}()
				task(ctx)
			}()
		case <-ctx.Done():
			return
		}
	}
}

// RunBackground 启动 hint、探测和 gossip 后台任务。
func (n *Node) RunBackground(ctx context.Context) {
	tasks := []backgroundTask{
		{
			name:     "hint handoff",
			interval: 2 * time.Second,
			run:      n.flushHints,
		},
		{
			name:     "failure detector",
			interval: 1 * time.Second,
			run:      n.probeOnce,
		},
		{
			name:     "gossip",
			interval: 2 * time.Second,
			run:      n.gossipOnce,
		},
	}

	for _, task := range tasks {
		go n.runPeriodicTask(
			ctx,
			task.name,
			task.interval,
			task.run,
		)
	}
}
