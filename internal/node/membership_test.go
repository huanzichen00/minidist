package node

import "testing"

func newMembershipForTest(state memberState) *failureDetector {
	return &failureDetector{
		self: "node-a",
		members: map[string]memberState{
			"node-a": {Status: statusAlive},
			"node-b": state,
		},
	}
}

func TestMembershipMergeUsesHigherVersionInSameIncarnation(t *testing.T) {
	fd := newMembershipForTest(memberState{
		Status:      statusAlive,
		Incarnation: 5,
		Version:     2,
	})

	fd.Merge([]gossipMember{{
		Node:        "node-b",
		Status:      statusDead.String(),
		Incarnation: 5,
		Version:     3,
	}})

	got := fd.members["node-b"]
	if got.Status != statusDead || got.Incarnation != 5 || got.Version != 3 {
		t.Fatalf("expected dead (5,3), got %s (%d,%d)", got.Status, got.Incarnation, got.Version)
	}
}

func TestMembershipMergePrefersHigherIncarnation(t *testing.T) {
	fd := newMembershipForTest(memberState{
		Status:      statusDead,
		Incarnation: 5,
		Version:     100,
	})

	fd.Merge([]gossipMember{{
		Node:        "node-b",
		Status:      statusAlive.String(),
		Incarnation: 6,
		Version:     1,
	}})

	got := fd.members["node-b"]
	if got.Status != statusAlive || got.Incarnation != 6 || got.Version != 1 {
		t.Fatalf("expected alive (6,1), got %s (%d,%d)", got.Status, got.Incarnation, got.Version)
	}
}

func TestMembershipMergeIgnoresOlderIncarnation(t *testing.T) {
	fd := newMembershipForTest(memberState{
		Status:      statusAlive,
		Incarnation: 6,
		Version:     1,
	})

	fd.Merge([]gossipMember{{
		Node:        "node-b",
		Status:      statusDead.String(),
		Incarnation: 5,
		Version:     999,
	}})

	got := fd.members["node-b"]
	if got.Status != statusAlive || got.Incarnation != 6 || got.Version != 1 {
		t.Fatalf("expected local alive (6,1) to remain, got %s (%d,%d)", got.Status, got.Incarnation, got.Version)
	}
}

func TestMembershipSelfRefutation(t *testing.T) {
	fd := &failureDetector{
		self: "node-a",
		members: map[string]memberState{
			"node-a": {
				Status:      statusAlive,
				Incarnation: 5,
				Version:     2,
			},
		},
	}

	fd.Merge([]gossipMember{{
		Node:        "node-a",
		Status:      statusSuspect.String(),
		Incarnation: 5,
		Version:     3,
	}})

	got := fd.members["node-a"]
	if got.Status != statusAlive || got.Incarnation != 6 || got.Version != 1 {
		t.Fatalf("expected alive (6,1), got %s (%d,%d)", got.Status, got.Incarnation, got.Version)
	}
}
