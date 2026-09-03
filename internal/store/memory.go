package store

import "sync"

type Memory struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemory() *Memory {
	return &Memory{
		data: make(map[string][]byte),
	}
}

func (m *Memory) Set(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
}

func (m *Memory) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	return value, ok
}
