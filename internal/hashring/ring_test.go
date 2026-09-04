package hashring

import (
	"fmt"
	"testing"
)

func TestRingGet(t *testing.T) {
	nodes := []string{
		"node-a",
		"node-b",
		"node-c",
	}

	ring := New(nodes, 100)

	first := ring.Get("hello")

	for range 100 {
		got := ring.Get("hello")
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
		node := ring.Get(key)

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
		node := ring.Get(key)
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
		node := ring.Get(key)

		if node == "node-b" {
			t.Fatal("removed node returned by ring")
		}
	}
}
