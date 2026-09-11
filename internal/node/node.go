package node

import (
	"minidist/internal/hashring"
	"minidist/internal/store"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Node struct {
	addr string

	ring  *hashring.Ring
	store *store.Memory

	virtualNodes int

	client *http.Client

	// N
	replicas int
	// W
	writeQuorum int
	// R
	readQuorum int

	version       atomic.Uint64
	configVersion atomic.Uint64
	configMu      sync.Mutex

	hints *hintStore

	fd *failureDetector
}

func New(addr string, nodes []string, walPath string) (*Node, error) {
	memory, err := store.OpenMemory(walPath)
	if err != nil {
		return nil, err
	}
	n := &Node{
		addr:         addr,
		ring:         hashring.New(nodes, 100),
		store:        memory,
		virtualNodes: 100,

		client: &http.Client{
			Timeout: 2 * time.Second,
		},

		replicas:    3,
		writeQuorum: 2,
		readQuorum:  2,

		hints: newHintStore(),
		fd:    newFailureDetector(nodes, addr),
	}

	n.version.Store(memory.MaxVersionCounter())

	return n, err
}

func (n *Node) Close() error {
	return n.store.Close()
}
