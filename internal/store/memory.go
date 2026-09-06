package store

import "sync"

type Value struct {
	Data    []byte
	Version uint64
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

	m.data[key] = value
}

func (m *Memory) Get(key string) (Value, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	return value, ok
}
