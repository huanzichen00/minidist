package store

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"sync"
)

const defaultSnapshotThreshold int64 = 64 * 1024 * 1024

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

	snapshotPath      string
	snapshotThreshold int64
	// 保证同一时间只有一个 snapshot 任务
	snapshotMu sync.Mutex

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

	current, ok := m.data[key]
	if ok {
		if CompareVersion(value.Version, current.Version) <= 0 {
			m.mu.Unlock()
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
			m.mu.Unlock()
			return err
		}

		if err = m.wal.Append(data); err != nil {
			m.mu.Unlock()
			return err
		}
	}

	m.data[key] = value

	if value.Version.Counter > m.maxVersionCounter {
		m.maxVersionCounter = value.Version.Counter
	}

	m.mu.Unlock()

	if err := m.MaybeSnapshot(); err != nil {
		log.Printf("maybe snapshot failed: %v", err)
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

	current, ok := m.data[key]
	if !ok {
		m.mu.Unlock()
		return false, nil
	}

	if CompareVersion(current.Version, version) != 0 {
		m.mu.Unlock()
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
			m.mu.Unlock()
			return false, err
		}

		if err := m.wal.Append(data); err != nil {
			m.mu.Unlock()
			return false, err
		}
	}

	delete(m.data, key)
	m.mu.Unlock()

	if err := m.MaybeSnapshot(); err != nil {
		log.Printf("maybe snapshot failed: %v", err)
	}

	return true, nil
}

func OpenMemory(path string, snapshotPath string) (*Memory, error) {
	snapshot, err := loadSnapshot(snapshotPath)
	if err != nil {
		return nil, err
	}

	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}

	m := &Memory{
		data:              make(map[string]Value, len(snapshot.Data)),
		wal:               wal,
		snapshotPath:      snapshotPath,
		snapshotThreshold: defaultSnapshotThreshold,
		maxVersionCounter: snapshot.MaxVersionCounter,
	}
	maps.Copy(m.data, snapshot.Data)

	replayStats, err := wal.Replay(func(data []byte) error {
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
	})
	if err != nil {
		_ = wal.Close()
		return nil, err
	}

	log.Printf("store recovery complete: snapshot_keys=%d wal_records=%d tail_repaired=%t keys=%d max_version=%d", len(snapshot.Data), replayStats.Records, replayStats.TailRepaired, len(m.data), m.maxVersionCounter)
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

func (m *Memory) saveSnapshot() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := snapshotData{
		Data:              make(map[string]Value, len(m.data)),
		MaxVersionCounter: m.maxVersionCounter,
	}

	maps.Copy(snapshot.Data, m.data)

	if err := saveSnapshot(m.snapshotPath, snapshot); err != nil {
		return err
	}

	if err := m.wal.Truncate(); err != nil {
		return err
	}

	return nil
}

func (m *Memory) MaybeSnapshot() error {
	if m.wal == nil {
		return nil
	}

	if !m.snapshotMu.TryLock() {
		return nil
	}
	defer m.snapshotMu.Unlock()

	size, err := m.wal.Size()
	if err != nil {
		return err
	}

	if size < m.snapshotThreshold {
		return nil
	}

	return m.saveSnapshot()
}

func (m *Memory) SaveSnapshot() error {
	m.snapshotMu.Lock()
	defer m.snapshotMu.Unlock()

	return m.saveSnapshot()
}

func (m *Memory) WALSize() (int64, error) {
	if m.wal == nil {
		return 0, nil
	}

	return m.wal.Size()
}
