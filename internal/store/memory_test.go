package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCompareVersionUsesCounter(t *testing.T) {
	if got := CompareVersion(
		Version{Counter: 1, NodeID: "node-a"},
		Version{Counter: 2, NodeID: "node-a"},
	); got >= 0 {
		t.Fatalf("expected lower counter to compare smaller, got %d", got)
	}
}

func TestCompareVersionUsesNodeIDAsTieBreak(t *testing.T) {
	if got := CompareVersion(
		Version{Counter: 10, NodeID: "node-a"},
		Version{Counter: 10, NodeID: "node-b"},
	); got >= 0 {
		t.Fatalf("expected node-a to compare smaller than node-b, got %d", got)
	}

	if got := CompareVersion(
		Version{Counter: 10, NodeID: "node-b"},
		Version{Counter: 10, NodeID: "node-a"},
	); got <= 0 {
		t.Fatalf("expected node-b to compare larger than node-a, got %d", got)
	}
}

func TestMemoryOlderValueCannotOverwriteNewer(t *testing.T) {
	memory := NewMemory()
	memory.Set("foo", Value{
		Data:    []byte("new"),
		Version: Version{Counter: 10, NodeID: "node-a"},
	})
	memory.Set("foo", Value{
		Data:    []byte("old"),
		Version: Version{Counter: 5, NodeID: "node-a"},
	})

	got, ok := memory.Get("foo")
	if !ok {
		t.Fatal("expected value to exist")
	}
	if string(got.Data) != "new" || got.Version.Counter != 10 {
		t.Fatalf("expected v10 new, got version=%d data=%q", got.Version.Counter, got.Data)
	}
}

func TestMemoryTombstoneOverwritesOlderValue(t *testing.T) {
	memory := NewMemory()
	memory.Set("foo", Value{
		Data:    []byte("value"),
		Version: Version{Counter: 5, NodeID: "node-a"},
	})
	memory.Set("foo", Value{
		Version: Version{Counter: 6, NodeID: "node-a"},
		Deleted: true,
	})

	got, ok := memory.Get("foo")
	if !ok || !got.Deleted || got.Version.Counter != 6 {
		t.Fatalf("expected tombstone v6, got found=%t deleted=%t version=%d", ok, got.Deleted, got.Version.Counter)
	}
}

func TestOlderValueCannotResurrectTombstone(t *testing.T) {
	memory := NewMemory()
	memory.Set("foo", Value{
		Version: Version{Counter: 10, NodeID: "node-a"},
		Deleted: true,
	})
	memory.Set("foo", Value{
		Data:    []byte("old"),
		Version: Version{Counter: 9, NodeID: "node-a"},
	})

	got, ok := memory.Get("foo")
	if !ok || !got.Deleted || got.Version.Counter != 10 {
		t.Fatalf("expected tombstone v10 to remain, got found=%t deleted=%t version=%d", ok, got.Deleted, got.Version.Counter)
	}
}

func TestMemoryDeleteIfMatch(t *testing.T) {
	memory := NewMemory()
	value := Value{
		Data:    []byte("value"),
		Version: Version{Counter: 10, NodeID: "node-a"},
	}
	memory.Set("foo", value)

	deleted, err := memory.DeleteIfMatch("foo", Version{Counter: 9, NodeID: "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted {
		t.Fatal("expected mismatched version not to delete value")
	}
	if _, ok := memory.Get("foo"); !ok {
		t.Fatal("expected value to remain after mismatched delete")
	}

	deleted, err = memory.DeleteIfMatch("foo", value.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected matching version to delete value")
	}
	if _, ok := memory.Get("foo"); ok {
		t.Fatal("expected value to be deleted")
	}
}

func TestMemoryWALReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	memory, err := OpenMemory(path)
	if err != nil {
		t.Fatal(err)
	}

	value := Value{
		Data:    []byte("value"),
		Version: Version{Counter: 7, NodeID: "node-a"},
	}
	if err := memory.Set("deleted", value); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.DeleteIfMatch("deleted", value.Version); err != nil {
		t.Fatal(err)
	}

	tombstone := Value{
		Version: Version{Counter: 11, NodeID: "node-a"},
		Deleted: true,
	}
	if err := memory.Set("tombstone", tombstone); err != nil {
		t.Fatal(err)
	}
	if err := memory.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenMemory(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	if _, ok := recovered.Get("deleted"); ok {
		t.Fatal("expected deleted key to stay deleted after replay")
	}

	got, ok := recovered.Get("tombstone")
	if !ok || !got.Deleted || got.Version.Counter != 11 {
		t.Fatalf("recovered tombstone = %#v, found=%t", got, ok)
	}

	if got := recovered.MaxVersionCounter(); got != 11 {
		t.Fatalf("max version counter = %d, want 11", got)
	}
}

func TestMemoryWALReplayIgnoresTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")

	memory, err := OpenMemory(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.Set("foo", Value{
		Data:    []byte("value"),
		Version: Version{Counter: 1, NodeID: "node-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := memory.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := OpenMemory(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	got, ok := recovered.Get("foo")
	if !ok || string(got.Data) != "value" {
		t.Fatalf("recovered value = %#v, found=%t", got, ok)
	}
}
