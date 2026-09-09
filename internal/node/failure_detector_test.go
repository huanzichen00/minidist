package node

import "testing"

func TestMergeRefutesNewerSuspicionOfSelf(t *testing.T) {
	fd := newFailureDetector([]string{"node-a"}, "node-a")
	before := fd.members["node-a"]

	fd.Merge([]gossipMember{{
		Node:        "node-a",
		Status:      statusSuspect.String(),
		Incarnation: before.Incarnation,
		Version:     before.Version + 1,
	}})

	got := fd.members["node-a"]
	if got.Status != statusAlive {
		t.Fatalf("expected self to refute suspicion as alive, got %s", got.Status)
	}
	if got.Incarnation != before.Incarnation+1 {
		t.Fatalf("expected incarnation %d, got %d", before.Incarnation+1, got.Incarnation)
	}
}

func TestMergeDoesNotRefuteHealthySelfGossip(t *testing.T) {
	fd := newFailureDetector([]string{"node-a"}, "node-a")
	before := fd.members["node-a"]

	fd.Merge([]gossipMember{{
		Node:        "node-a",
		Status:      statusAlive.String(),
		Incarnation: before.Incarnation,
		Version:     before.Version + 1,
	}})

	got := fd.members["node-a"]
	if got.Incarnation != before.Incarnation {
		t.Fatalf("expected incarnation to remain %d, got %d", before.Incarnation, got.Incarnation)
	}
}
