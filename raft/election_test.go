package raft

import "testing"

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	state := PersistentState{CurrentTerm: 5, VotedFor: ""}
	log := NewLog()

	newState, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 3, CandidateID: "node2",
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected for a stale term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
	if newState != state {
		t.Fatalf("state changed on a rejected stale-term request: got %+v, want unchanged %+v", newState, state)
	}
}

func TestRequestVoteGrantsWhenNoPriorVoteAndLogUpToDate(t *testing.T) {
	state := PersistentState{CurrentTerm: 1, VotedFor: ""}
	log := NewLog()

	newState, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 1, CandidateID: "node2",
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}
	if newState.VotedFor != "node2" {
		t.Fatalf("VotedFor = %q, want %q", newState.VotedFor, "node2")
	}
}

func TestRequestVoteRejectsIfAlreadyVotedForDifferentCandidateSameTerm(t *testing.T) {
	state := PersistentState{CurrentTerm: 1, VotedFor: "node1"}
	log := NewLog()

	newState, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 1, CandidateID: "node2",
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected, already voted for a different candidate")
	}
	if newState.VotedFor != "node1" {
		t.Fatalf("VotedFor changed to %q, want unchanged %q", newState.VotedFor, "node1")
	}
}

func TestRequestVoteGrantsAgainForSameCandidateSameTerm(t *testing.T) {
	state := PersistentState{CurrentTerm: 1, VotedFor: "node2"}
	log := NewLog()

	_, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 1, CandidateID: "node2",
	})

	if !reply.VoteGranted {
		t.Fatal("expected a repeated request from the same already-voted-for candidate to be granted")
	}
}

func TestRequestVoteHigherTermResetsVoteAndGrants(t *testing.T) {
	state := PersistentState{CurrentTerm: 1, VotedFor: "node1"}
	log := NewLog()

	newState, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 2, CandidateID: "node2",
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted after term bump resets the prior vote")
	}
	if newState.CurrentTerm != 2 {
		t.Fatalf("CurrentTerm = %d, want 2", newState.CurrentTerm)
	}
	if newState.VotedFor != "node2" {
		t.Fatalf("VotedFor = %q, want %q", newState.VotedFor, "node2")
	}
}

func TestRequestVoteRejectsWhenCandidateLogTermBehind(t *testing.T) {
	state := PersistentState{CurrentTerm: 2, VotedFor: ""}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	log.Append(2, Command{Op: "SET", Key: "b", Value: "2"})

	_, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 2, CandidateID: "node2", LastLogIndex: 5, LastLogTerm: 1,
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected: candidate's last log term is behind the voter's")
	}
}

func TestRequestVoteRejectsWhenSameTermButShorterLog(t *testing.T) {
	state := PersistentState{CurrentTerm: 2, VotedFor: ""}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	log.Append(2, Command{Op: "SET", Key: "b", Value: "2"})
	log.Append(2, Command{Op: "SET", Key: "c", Value: "3"})

	_, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 2, CandidateID: "node2", LastLogIndex: 2, LastLogTerm: 2,
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected: same last term but candidate's log is shorter")
	}
}

func TestRequestVoteGrantsWhenCandidateLogTermAhead(t *testing.T) {
	state := PersistentState{CurrentTerm: 1, VotedFor: ""}
	log := NewLog()
	for i := 0; i < 5; i++ {
		log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	}

	newState, reply := HandleRequestVote(state, log, RequestVoteArgs{
		Term: 3, CandidateID: "node2", LastLogIndex: 1, LastLogTerm: 2,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted: candidate's last log term is ahead, even with a shorter log")
	}
	if newState.CurrentTerm != 3 {
		t.Fatalf("CurrentTerm = %d, want 3", newState.CurrentTerm)
	}
}
