package hashring

import (
	"hash/crc32"
	"sort"
)

type Ring struct {
	nodes  map[uint32]string
	hashes []uint32
}

func New(nodes []string) *Ring {
	r := &Ring{
		nodes: make(map[uint32]string),
	}

	// 构造哈希环
	for _, node := range nodes {
		h := hash(node)

		r.nodes[h] = node
		r.hashes = append(r.hashes, h)
	}

	// 按哈希值由小到大排序
	sort.Slice(r.hashes, func(i, j int) bool {
		return r.hashes[i] < r.hashes[j]
	})

	return r
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

func hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}
