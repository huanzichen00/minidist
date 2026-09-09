package hashring

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type Ring struct {
	mu sync.RWMutex

	virtualNodes int
	nodes        map[uint32]string
	hashes       []uint32
	members      map[string]struct{}
}

func New(nodes []string, virtualNodes int) *Ring {
	r := &Ring{
		virtualNodes: virtualNodes,
		nodes:        make(map[uint32]string),
		members:      make(map[string]struct{}),
	}

	// 构造哈希环
	for _, node := range nodes {
		r.Add(node)
	}

	return r
}

func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.members[node]; ok {
		return
	}
	r.members[node] = struct{}{}

	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := fmt.Sprintf("%s#%d", node, i)

		h := hash(virtualNode)

		r.nodes[h] = node
		r.hashes = append(r.hashes, h)
	}

	sort.Slice(r.hashes, func(i, j int) bool {
		return r.hashes[i] < r.hashes[j]
	})
}

func (r *Ring) get(key string) (string, bool) {
	if len(r.hashes) == 0 {
		return "", false
	}

	h := hash(key)

	// 返回>=h的最小节点哈希值的索引
	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})

	// 索引值等于哈希空间长度越界，返回环第一个节点
	if idx == len(r.hashes) {
		idx = 0
	}

	return r.nodes[r.hashes[idx]], true
}

func (r *Ring) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.get(key)
}

func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]string, 0, len(r.members))
	for member := range r.members {
		members = append(members, member)
	}

	sort.Strings(members)
	return members
}

func (r *Ring) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 || n <= 0 {
		return nil
	}

	if n > len(r.members) {
		n = len(r.members)
	}

	h := hash(key)

	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})

	if idx == len(r.hashes) {
		idx = 0
	}

	result := make([]string, 0, n)
	seen := make(map[string]struct{})

	// 沿哈希环顺时针查找，收集不同的真实节点
	for len(result) < n && len(seen) < len(r.nodes) {
		node := r.nodes[r.hashes[idx]]

		// 同一个真实节点可能对应多个虚拟节点，避免重复加入
		if _, ok := seen[node]; !ok {
			seen[node] = struct{}{}
			result = append(result, node)
		}

		idx++
		if idx == len(r.hashes) {
			idx = 0
		}
	}
	return result
}

func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.members[node]; !ok {
		return
	}

	delete(r.members, node)

	remove := make(map[uint32]struct{})

	for i := range r.virtualNodes {
		virtualNode := fmt.Sprintf("%s#%d", node, i)

		h := hash(virtualNode)
		delete(r.nodes, h)
		remove[h] = struct{}{}
	}

	// 删除 r.hashes 中已删除 node 的哈希值
	// 复用原 slice 的低层数组, 筛选后重新写入源 slice
	hashes := r.hashes[:0]
	for _, h := range r.hashes {
		if _, ok := remove[h]; !ok {
			hashes = append(hashes, h)
		}
	}

	r.hashes = hashes
}

func hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}
