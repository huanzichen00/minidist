package node

import (
	"minidist/internal/store"
	"testing"
)

func hintValue(counter uint64, data string) store.Value {
	return store.Value{
		Data: []byte(data),
		Version: store.Version{
			Counter: counter,
			NodeID:  "node-a",
		},
	}
}

func singleHint(t *testing.T, h *hintStore) hint {
	t.Helper()

	hints := h.List()
	if len(hints) != 1 {
		t.Fatalf("expected one hint, got %d", len(hints))
	}

	return hints[0]
}

func TestHintStoreKeepsLatestVersion(t *testing.T) {
	hints := newHintStore()

	hints.Add("node-b", "foo", hintValue(1, "v1"))
	hints.Add("node-b", "foo", hintValue(2, "v2"))
	hints.Add("node-b", "foo", hintValue(3, "v3"))

	got := singleHint(t, hints)
	if got.Value.Version.Counter != 3 || string(got.Value.Data) != "v3" {
		t.Fatalf("expected v3, got version=%d data=%q", got.Value.Version.Counter, got.Value.Data)
	}
}

func TestHintStoreOlderVersionCannotOverwriteNewer(t *testing.T) {
	hints := newHintStore()

	hints.Add("node-b", "foo", hintValue(10, "new"))
	hints.Add("node-b", "foo", hintValue(5, "old"))

	got := singleHint(t, hints)
	if got.Value.Version.Counter != 10 || string(got.Value.Data) != "new" {
		t.Fatalf("expected v10 new, got version=%d data=%q", got.Value.Version.Counter, got.Value.Data)
	}
}

func TestHintStoreRemoveIfMatchDeletesMatchingVersion(t *testing.T) {
	hints := newHintStore()
	value := hintValue(10, "value")
	hints.Add("node-b", "foo", value)

	hints.RemoveIfMatch("node-b", "foo", value.Version)

	if got := hints.List(); len(got) != 0 {
		t.Fatalf("expected hint store to be empty, got %d hints", len(got))
	}
}

func TestHintStoreRemoveIfMatchDoesNotRemoveNewerHint(t *testing.T) {
	hints := newHintStore()
	v10 := hintValue(10, "v10")
	v11 := hintValue(11, "v11")

	hints.Add("node-b", "foo", v10)
	hints.Add("node-b", "foo", v11)
	hints.RemoveIfMatch("node-b", "foo", v10.Version)

	got := singleHint(t, hints)
	if got.Value.Version.Counter != 11 || string(got.Value.Data) != "v11" {
		t.Fatalf("expected v11 to remain, got version=%d data=%q", got.Value.Version.Counter, got.Value.Data)
	}
}
