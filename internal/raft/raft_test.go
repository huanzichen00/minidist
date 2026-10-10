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

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	r.mu.Lock()
	r.currentTerm = 3
	r.mu.Unlock()

	resp := r.RequestVote(RequestVoteRequest{
		Term:        2,
		CandidateID: "node-b",
	})

	if resp.VoteGranted {
		t.Fatal("stale candidate received vote")
	}

	if resp.Term != 3 {
		t.Fatalf("response term = %d, want 3", resp.Term)
	}
}

func TestRequestVoteUpdatesHigherTerm(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	r.mu.Lock()
	r.currentTerm = 2
	r.role = Leader
	r.votedFor = "node-a"
	r.mu.Unlock()

	resp := r.RequestVote(RequestVoteRequest{
		Term:         3,
		CandidateID:  "node-b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Fatal("vote not granted")
	}

	if r.CurrentTerm() != 3 {
		t.Fatalf("current term = %d, want 3", r.CurrentTerm())
	}

	if r.Role() != Follower {
		t.Fatalf("role = %v, want follower", r.Role())
	}
}

func TestRequestVoteOnlyVotesOncePerTerm(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	first := r.RequestVote(RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !first.VoteGranted {
		t.Fatal("first vote not granted")
	}

	second := r.RequestVote(RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-c",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if second.VoteGranted {
		t.Fatal("second candidate received vote in same term")
	}
}

func TestRequestVoteAllowsRepeatedRequestFromSameCandidate(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	req := RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	if !r.RequestVote(req).VoteGranted {
		t.Fatal("first vote not granted")
	}

	if !r.RequestVote(req).VoteGranted {
		t.Fatal("repeated request from same candidate was rejected")
	}
}

func TestRequestVoteRejectsCandidateWithOlderLog(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	r.appendEntry(1, []byte("a"))
	r.appendEntry(2, []byte("b"))

	resp := r.RequestVote(RequestVoteRequest{
		Term:         3,
		CandidateID:  "node-b",
		LastLogIndex: 100,
		LastLogTerm:  1,
	})

	if resp.VoteGranted {
		t.Fatal("candidate with older log term received vote")
	}

	if r.CurrentTerm() != 3 {
		t.Fatalf("current term = %d, want 3", r.CurrentTerm())
	}
}

func TestRequestVotePrefersHigherLastLogTerm(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	r.appendEntry(1, []byte("a"))
	r.appendEntry(2, []byte("b"))

	resp := r.RequestVote(RequestVoteRequest{
		Term:         3,
		CandidateID:  "node-b",
		LastLogIndex: 1,
		LastLogTerm:  3,
	})

	if !resp.VoteGranted {
		t.Fatal("candidate with newer last log term was rejected")
	}
}

func TestStartElectionBecomesCandidateAndVotesForSelf(t *testing.T) {
	r := New("node-a", []string{"node-a", "node-b", "node-c"}, make(chan ApplyMsg, 8))

	req := r.startElection()

	if r.Role() != Candidate {
		t.Fatalf("role = %v, want candidate", r.role)
	}

	if r.currentTerm != 1 {
		t.Fatalf("term = %d, want 1", r.currentTerm)
	}

	r.mu.Lock()
	votedFor := r.votedFor
	r.mu.Unlock()

	if votedFor != "node-a" {
		t.Fatalf("votedFor = %q, want node-a", votedFor)
	}

	if req.Term != 1 {
		t.Fatalf("request term = %d, want 1", req.Term)
	}

	if req.CandidateID != "node-a" {
		t.Fatalf("candidate id = %q, want node-a", req.CandidateID)
	}
}

func TestStartElectionIncrementsTermEachRound(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	first := r.startElection()
	second := r.startElection()

	if first.Term != 1 {
		t.Fatalf("first election term = %d, want 1", first.Term)
	}

	if second.Term != 2 {
		t.Fatalf("second election term = %d, want 2", second.Term)
	}

	if r.CurrentTerm() != 2 {
		t.Fatalf("current term = %d, want 2", r.CurrentTerm())
	}
}

func TestStartElectionIncludesLastLogInfo(t *testing.T) {
	r := New("node-a", nil, make(chan ApplyMsg, 8))

	r.appendEntry(1, []byte("a"))
	r.appendEntry(2, []byte("b"))
	r.appendEntry(2, []byte("c"))

	req := r.startElection()

	if req.LastLogIndex != 3 {
		t.Fatalf("last log index = %d, want 3", req.LastLogIndex)
	}

	if req.LastLogTerm != 2 {
		t.Fatalf("last log term = %d, want 2", req.LastLogTerm)
	}
}
