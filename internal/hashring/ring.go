package hashring

import (
	"fmt"
	"hash/crc32"
	"sort"
)

type Ring struct {
	replicas int
	nodes    map[uint32]string
	hashes   []uint32
}

func New(nodes []string, replicas int) *Ring {
	r := &Ring{
		replicas: replicas,
		nodes:    make(map[uint32]string),
	}

	// 构造哈希环
	for _, node := range nodes {
		r.Add(node)
	}

	return r
}

func (r *Ring) Add(node string) {
	for i := 0; i < r.replicas; i++ {
		virtualNode := fmt.Sprintf("%s#%d", node, i)

		h := hash(virtualNode)

		r.nodes[h] = node
		r.hashes = append(r.hashes, h)
	}

	sort.Slice(r.hashes, func(i, j int) bool {
		return r.hashes[i] < r.hashes[j]
	})
}

func (r *Ring) Get(key string) string {
	if len(r.hashes) == 0 {
		return ""
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

	return r.nodes[r.hashes[idx]]
}

func (r *Ring) Remove(node string) {
	remove := make(map[uint32]struct{})

	for i := range r.replicas {
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
