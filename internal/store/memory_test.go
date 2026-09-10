package store

import "testing"

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

	if memory.DeleteIfMatch("foo", Version{Counter: 9, NodeID: "node-a"}) {
		t.Fatal("expected mismatched version not to delete value")
	}
	if _, ok := memory.Get("foo"); !ok {
		t.Fatal("expected value to remain after mismatched delete")
	}

	if !memory.DeleteIfMatch("foo", value.Version) {
		t.Fatal("expected matching version to delete value")
	}
	if _, ok := memory.Get("foo"); ok {
		t.Fatal("expected value to be deleted")
	}
}
