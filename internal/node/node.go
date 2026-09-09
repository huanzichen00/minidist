package node

import (
	"minidist/internal/hashring"
	"minidist/internal/store"
	"net/http"
	"sync/atomic"
	"time"
)

type Node struct {
	addr string

	ring  *hashring.Ring
	store *store.Memory

	client *http.Client

	// N
	replicas int
	// W
	writeQuorum int
	// R
	readQuorum int

	version atomic.Uint64

	hints *hintStore

	fd *failureDetector
}

func New(addr string, nodes []string) *Node {
	return &Node{
		addr:  addr,
		ring:  hashring.New(nodes, 100),
		store: store.NewMemory(),

		client: &http.Client{
			Timeout: 2 * time.Second,
		},

		replicas:    3,
		writeQuorum: 2,
		readQuorum:  2,

		hints: newHintStore(),
		fd:    newFailureDetector(nodes, addr),
	}
}
