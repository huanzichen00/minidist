package store

import (
	"encoding/json"
	"fmt"
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
	wal  *WAL

	maxVersionCounter uint64
}

type walOp string

const (
	walOpSet    walOp = "set"
	walOpDelete walOp = "delete"
)

type kvWALEntry struct {
	Op      walOp   `json:"op"`
	Key     string  `json:"key"`
	Value   Value   `json:"value"`
	Version Version `json:"version"`
}

func NewMemory() *Memory {
	return &Memory{
		data: make(map[string]Value),
	}
}

func (m *Memory) Set(key string, value Value) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.data[key]
	if ok {
		if CompareVersion(value.Version, current.Version) <= 0 {
			return nil
		}
	}

	if m.wal != nil {
		entry := kvWALEntry{
			Op:    walOpSet,
			Key:   key,
			Value: value,
		}

		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		if err = m.wal.Append(data); err != nil {
			return err
		}
	}

	m.data[key] = value

	if value.Version.Counter > m.maxVersionCounter {
		m.maxVersionCounter = value.Version.Counter
	}

	return nil
}

func (m *Memory) ForceSet(key string, value Value) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value

	if value.Version.Counter > m.maxVersionCounter {
		m.maxVersionCounter = value.Version.Counter
	}
}

func (m *Memory) forceDelete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
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

func (m *Memory) DeleteIfMatch(key string, version Version) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.data[key]
	if !ok {
		return false, nil
	}

	if CompareVersion(current.Version, version) != 0 {
		return false, nil
	}

	if m.wal != nil {
		entry := kvWALEntry{
			Op:      walOpDelete,
			Key:     key,
			Version: version,
		}

		data, err := json.Marshal(entry)
		if err != nil {
			return false, err
		}

		if err := m.wal.Append(data); err != nil {
			return false, err
		}
	}

	delete(m.data, key)
	return true, nil
}

func OpenMemory(path string) (*Memory, error) {
	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}

	m := &Memory{
		data: make(map[string]Value),
		wal:  wal,
	}

	if err := wal.Replay(func(data []byte) error {
		var entry kvWALEntry

		if err := json.Unmarshal(data, &entry); err != nil {
			return err
		}

		switch entry.Op {
		case walOpSet:
			m.ForceSet(entry.Key, entry.Value)

		case walOpDelete:
			current, ok := m.Get(entry.Key)
			if !ok {
				return nil
			}

			if CompareVersion(current.Version, entry.Version) == 0 {
				m.forceDelete(entry.Key)
			}

		default:
			return fmt.Errorf("unknown wal operation %q", entry.Op)
		}

		return nil
	}); err != nil {
		_ = wal.Close()
		return nil, err
	}

	return m, nil
}

func (m *Memory) MaxVersionCounter() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.maxVersionCounter
}

func (m *Memory) Close() error {
	if m.wal == nil {
		return nil
	}

	return m.wal.Close()
}
