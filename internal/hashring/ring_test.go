package hashring

import (
	"fmt"
	"reflect"
	"testing"
)

func TestRingGet(t *testing.T) {
	nodes := []string{
		"node-a",
		"node-b",
		"node-c",
	}

	ring := New(nodes, 100)

	first, ok := ring.Get("hello")
	if !ok {
		t.Fatal("expected node for non-empty ring")
	}

	for range 100 {
		got, ok := ring.Get("hello")
		if !ok {
			t.Fatal("expected node for non-empty ring")
		}
		if got != first {
			t.Fatalf("expected %s, got %s", first, got)
		}
	}
}

func TestRingReturnKnownNode(t *testing.T) {
	nodes := []string{
		"node-a",
		"node-b",
		"node-c",
	}

	ring := New(nodes, 100)

	known := map[string]bool{
		"node-a": true,
		"node-b": true,
		"node-c": true,
	}

	for i := range 100 {
		key := fmt.Sprintf("key-%d", i)
		node, ok := ring.Get(key)
		if !ok {
			t.Fatalf("expected node for key %q", key)
		}

		if !known[node] {
			t.Fatalf("unknown node: %s", node)
		}
	}
}

func TestRingDistribution(t *testing.T) {
	nodes := []string{
		"node-a",
		"node-b",
		"node-c",
	}

	ring := New(nodes, 100)

	count := make(map[string]int)
	for i := range 100000 {
		key := fmt.Sprintf("key-%d", i)
		node, ok := ring.Get(key)
		if !ok {
			t.Fatalf("expected node for key %q", key)
		}
		count[node]++
	}

	for node, n := range count {
		t.Logf("%s: %d", node, n)
	}
}

func TestRingRemove(t *testing.T) {
	nodes := []string{
		"node-a",
		"node-b",
		"node-c",
	}

	ring := New(nodes, 100)

	ring.Remove("node-b")

	for i := range 10000 {
		key := fmt.Sprintf("key-%d", i)
		node, ok := ring.Get(key)
		if !ok {
			t.Fatalf("expected node for key %q", key)
		}

		if node == "node-b" {
			t.Fatal("removed node returned by ring")
		}
	}
}

func TestRingMembers(t *testing.T) {
	ring := New([]string{"node-c", "node-a", "node-b"}, 100)

	got := ring.Members()
	want := []string{"node-a", "node-b", "node-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected members %v, got %v", want, got)
	}

	got[0] = "changed"
	if after := ring.Members(); !reflect.DeepEqual(after, want) {
		t.Fatalf("Members exposed internal data: got %v", after)
	}

	ring.Add("node-d")
	ring.Add("node-a")
	want = []string{"node-a", "node-b", "node-c", "node-d"}
	if got := ring.Members(); !reflect.DeepEqual(got, want) {
		t.Fatalf("expected members %v, got %v", want, got)
	}
}
