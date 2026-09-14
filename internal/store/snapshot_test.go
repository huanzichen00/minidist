package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newTestSnapshot 创建测试快照文件。
func newTestSnapshot(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "node.snapshot")
	if err := saveSnapshot(path, snapshotData{
		Data: map[string]Value{
			"foo": {
				Data:    []byte("value"),
				Version: Version{Counter: 1, NodeID: "node-a"},
			},
		},
		MaxVersionCounter: 1,
	}); err != nil {
		t.Fatal(err)
	}

	return path
}

// TestLoadSnapshotRejectsInvalidMagic 验证快照 magic 损坏会失败。
func TestLoadSnapshotRejectsInvalidMagic(t *testing.T) {
	path := newTestSnapshot(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[0]++
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err = loadSnapshot(path)
	if err == nil || !strings.Contains(err.Error(), "invalid snapshot magic") {
		t.Fatalf("load error = %v, want invalid snapshot magic", err)
	}
}

// TestLoadSnapshotRejectsUnsupportedVersion 验证不支持的版本会失败。
func TestLoadSnapshotRejectsUnsupportedVersion(t *testing.T) {
	path := newTestSnapshot(t)
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var version [2]byte
	binary.BigEndian.PutUint16(version[:], snapshotVersion+1)
	if _, err := file.WriteAt(version[:], 4); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = loadSnapshot(path)
	if err == nil || !strings.Contains(err.Error(), "unsupported snapshot version") {
		t.Fatalf("load error = %v, want unsupported snapshot version", err)
	}
}

// TestLoadSnapshotRejectsChecksumMismatch 验证快照校验和损坏会失败。
func TestLoadSnapshotRejectsChecksumMismatch(t *testing.T) {
	path := newTestSnapshot(t)
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0}, 12); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = loadSnapshot(path)
	if err == nil || !strings.Contains(err.Error(), "snapshot checksum mismatch") {
		t.Fatalf("load error = %v, want snapshot checksum mismatch", err)
	}
}

// TestLoadSnapshotRejectsTruncatedFile 验证快照截断会失败。
func TestLoadSnapshotRejectsTruncatedFile(t *testing.T) {
	path := newTestSnapshot(t)
	if err := os.Truncate(path, snapshotHeaderSize-1); err != nil {
		t.Fatal(err)
	}

	_, err := loadSnapshot(path)
	if err == nil || !strings.Contains(err.Error(), "read snapshot header") {
		t.Fatalf("load error = %v, want read snapshot header", err)
	}
}
