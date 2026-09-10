package store

import (
	"maps"
	"sync"
)

type Version struct {
	Counter uint64 `json:"counter"`
	NodeID  string `json:"node_id"`
}

type Value struct {
	Data    []byte  `json:"data"`
	Version Version `json:"version"`
	Deleted bool    `json:"deleted"`
}

type Memory struct {
	mu   sync.RWMutex
	data map[string]Value
}

func NewMemory() *Memory {
	return &Memory{
		data: make(map[string]Value),
	}
}

func (m *Memory) Set(key string, value Value) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.data[key]
	if ok {
		if CompareVersion(value.Version, current.Version) <= 0 {
			return
		}
	}

	m.data[key] = value
}

func (m *Memory) ForceSet(key string, value Value) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
}

func (m *Memory) Get(key string) (Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	return value, ok
}

func CompareVersion(a, b Version) int {
	if a.Counter < b.Counter {
		return -1
	}

	if a.Counter > b.Counter {
		return 1
	}

	if a.NodeID < b.NodeID {
		return -1
	}

	if a.NodeID > b.NodeID {
		return 1
	}

	return 0
}

func (m *Memory) Snapshot() map[string]Value {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]Value, len(m.data))

	maps.Copy(result, m.data)

	return result
}

func (m *Memory) DeleteIfMatch(key string, version Version) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.data[key]
	if !ok {
		return false
	}

	if CompareVersion(current.Version, version) != 0 {
		return false
	}

	delete(m.data, key)
	return true
}
