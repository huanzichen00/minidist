package node

import (
	"minidist/internal/chunk"
	"minidist/internal/hashring"
	"minidist/internal/object"
	"minidist/internal/store"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Node 是协调 quorum、成员状态和本地存储的集群节点。
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

	objects *object.Store
}

// New 使用地址、初始成员和 WAL 路径创建节点。
func New(addr string, nodes []string, walPath string) (*Node, error) {
	memory, err := store.OpenMemory(walPath, walPath+".snapshot")
	if err != nil {
		return nil, err
	}

	chunkStore, err := chunk.Open(walPath + ".chunks")
	if err != nil {
		_ = memory.Close()
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
	n.objects = object.New(chunkStore, n)

	return n, nil
}

// Close 关闭节点持有的本地存储。
func (n *Node) Close() error {
	return n.store.Close()
}

// SaveSnapshot 请求本地存储立即保存快照。
func (n *Node) SaveSnapshot() error {
	return n.store.SaveSnapshot()
}
