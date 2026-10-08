package raft

import (
	"bytes"
	"testing"
)

func TestTruncateFromRejectsDummyEntry(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	if err := r.truncateFrom(0); err == nil {
		t.Fatal("truncateFrom(0) succeeded, want error")
	}

	if got := r.LastLogIndex(); got != 0 {
		t.Fatalf("last log index = %d, want 0", got)
	}
}

func TestTruncateFromRejectsOutOfRange(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))
	r.appendEntry(1, []byte("a"))

	if err := r.truncateFrom(2); err == nil {
		t.Fatal("truncateFrom(2) succeeded, want error")
	}

	if got := r.LastLogIndex(); got != 1 {
		t.Fatalf("last log index = %d, want 1", got)
	}
}

func TestTruncateFromRejectsCommittedLog(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))
	r.appendEntry(1, []byte("a"))
	r.appendEntry(1, []byte("b"))
	r.appendEntry(2, []byte("c"))

	if err := r.advanceCommit(2); err != nil {
		t.Fatal(err)
	}

	if err := r.truncateFrom(2); err == nil {
		t.Fatal("truncateFrom(2) succeeded, want error")
	}

	if got := r.LastLogIndex(); got != 3 {
		t.Fatalf("last log index = %d, want 3", got)
	}
}

func TestAdvanceCommitStopsAtRequestedIndex(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))
	r.appendEntry(1, []byte("a"))
	r.appendEntry(1, []byte("b"))
	r.appendEntry(2, []byte("c"))
	r.appendEntry(2, []byte("d"))

	if err := r.advanceCommit(2); err != nil {
		t.Fatal(err)
	}

	if got := r.CommitIndex(); got != 2 {
		t.Fatalf("commit index = %d, want 2", got)
	}

	if got := r.LastLogIndex(); got != 4 {
		t.Fatalf("last log index = %d, want 4", got)
	}

	if err := r.advanceCommit(5); err == nil {
		t.Fatal("advanceCommit(5) succeeded, want error")
	}

	if got := r.CommitIndex(); got != 2 {
		t.Fatalf("commit index changed after failed advance: %d, want 2", got)
	}
}

func TestApplyCommittedInOrderAndDoesNotRepeat(t *testing.T) {
	applyCh := make(chan ApplyMsg, 8)
	r := New("node-a", nil, applyCh)

	r.appendEntry(1, []byte("a"))
	r.appendEntry(1, []byte("b"))
	r.appendEntry(2, []byte("c"))

	if err := r.advanceCommit(3); err != nil {
		t.Fatal(err)
	}

	r.applyCommitted()

	want := []ApplyMsg{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
		{Index: 3, Term: 2, Command: []byte("c")},
	}

	if got := len(applyCh); got != len(want) {
		t.Fatalf("apply message count = %d, want %d", got, len(want))
	}

	for _, expected := range want {
		got := <-applyCh

		if got.Index != expected.Index {
			t.Fatalf("apply index = %d, want %d", got.Index, expected.Index)
		}

		if got.Term != expected.Term {
			t.Fatalf("apply term = %d, want %d", got.Term, expected.Term)
		}

		if !bytes.Equal(got.Command, expected.Command) {
			t.Fatalf("apply command = %q, want %q", got.Command, expected.Command)
		}
	}

	if got := r.LastApplied(); got != 3 {
		t.Fatalf("last applied = %d, want 3", got)
	}

	r.applyCommitted()

	if got := len(applyCh); got != 0 {
		t.Fatalf("second apply produced %d messages, want 0", got)
	}
}
