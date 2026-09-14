package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	snapshotMagic      uint32 = 0x4D445350 // "MDSP"
	snapshotVersion    uint16 = 1
	snapshotHeaderSize        = 16
	maxSnapshotSize    uint32 = 64 * 1024 * 1024
)

type snapshotData struct {
	Data              map[string]Value `json:"data"`
	MaxVersionCounter uint64           `json:"max_version_counter"`
}

// saveSnapshot 按 header、payload 顺序原子保存快照。
func saveSnapshot(path string, snapshot snapshotData) error {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if uint64(len(payload)) > uint64(maxSnapshotSize) {
		return fmt.Errorf("snapshot too large: %d bytes", len(payload))
	}

	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	var header [snapshotHeaderSize]byte

	binary.BigEndian.PutUint32(header[0:4], snapshotMagic)
	binary.BigEndian.PutUint16(header[4:6], snapshotVersion)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[12:16], crc32.ChecksumIEEE(payload))

	if _, err := file.Write(header[:]); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	return syncDir(path)
}

// loadSnapshot 校验 header 和 payload 后恢复快照。
func loadSnapshot(path string) (snapshotData, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return snapshotData{
			Data: make(map[string]Value),
		}, nil
	}
	if err != nil {
		return snapshotData{}, err
	}
	defer file.Close()

	var header [snapshotHeaderSize]byte

	if _, err := io.ReadFull(file, header[:]); err != nil {
		return snapshotData{}, fmt.Errorf("read snapshot header: %w", err)
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != snapshotMagic {
		return snapshotData{}, fmt.Errorf("invalid snapshot magic: got %08x", magic)
	}

	version := binary.BigEndian.Uint16(header[4:6])
	if version != snapshotVersion {
		return snapshotData{}, fmt.Errorf("unsupported snapshot version: %d", version)
	}

	length := binary.BigEndian.Uint32(header[8:12])
	if length > maxSnapshotSize {
		return snapshotData{}, fmt.Errorf("snapshot too large: %d bytes", length)
	}
	expectedChecksum := binary.BigEndian.Uint32(header[12:16])

	payload := make([]byte, length)

	if _, err := io.ReadFull(file, payload); err != nil {
		return snapshotData{}, fmt.Errorf("read snapshot payload: %w", err)
	}

	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return snapshotData{}, fmt.Errorf("snapshot checksum mismatch: expected %08x, got %08x", expectedChecksum, actualChecksum)
	}

	var snapshot snapshotData
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return snapshotData{}, err
	}

	if snapshot.Data == nil {
		snapshot.Data = make(map[string]Value)
	}

	return snapshot, nil
}

// syncDir 持久化目录项，确保 rename 结果可恢复。
func syncDir(path string) error {
	dir := filepath.Dir(path)

	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()

	return file.Sync()
}
