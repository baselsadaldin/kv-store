package raft

import "testing"

func TestStartElectionIncrementsTermAndVotesForSelf(t *testing.T) {
	state := PersistentState{CurrentTerm: 3, VotedFor: ""}
	log := NewLog()

	newState, args := StartElection(state, log, "node1")

	if newState.CurrentTerm != 4 {
		t.Fatalf("CurrentTerm = %d, want 4", newState.CurrentTerm)
	}
	if newState.VotedFor != "node1" {
		t.Fatalf("VotedFor = %q, want %q", newState.VotedFor, "node1")
	}
	if args.Term != 4 {
		t.Fatalf("args.Term = %d, want 4", args.Term)
	}
	if args.CandidateID != "node1" {
		t.Fatalf("args.CandidateID = %q, want %q", args.CandidateID, "node1")
	}
}

func TestStartElectionFromZeroState(t *testing.T) {
	state := PersistentState{}
	log := NewLog()

	newState, args := StartElection(state, log, "node1")

	if newState.CurrentTerm != 1 {
		t.Fatalf("CurrentTerm = %d, want 1", newState.CurrentTerm)
	}
	if args.Term != 1 {
		t.Fatalf("args.Term = %d, want 1", args.Term)
	}
}

func TestStartElectionArgsReflectLogState(t *testing.T) {
	state := PersistentState{CurrentTerm: 1}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	log.Append(1, Command{Op: "SET", Key: "b", Value: "2"})

	_, args := StartElection(state, log, "node1")

	if args.LastLogIndex != 2 {
		t.Fatalf("args.LastLogIndex = %d, want 2", args.LastLogIndex)
	}
	if args.LastLogTerm != 1 {
		t.Fatalf("args.LastLogTerm = %d, want 1", args.LastLogTerm)
	}
}

func TestCountVotesWinsWithMajorityOfThree(t *testing.T) {
	// 3-node cluster: self-vote + 1 granted reply = 2, majority is 2.
	replies := []RequestVoteReply{
		{Term: 4, VoteGranted: true},
	}
	if got := CountVotes(4, 3, replies); got != Won {
		t.Fatalf("CountVotes = %v, want Won", got)
	}
}

func TestCountVotesPendingWithoutMajorityOfFive(t *testing.T) {
	// 5-node cluster: self-vote + 1 granted reply = 2, majority is 3.
	replies := []RequestVoteReply{
		{Term: 4, VoteGranted: true},
		{Term: 4, VoteGranted: false},
	}
	if got := CountVotes(4, 5, replies); got != Pending {
		t.Fatalf("CountVotes = %v, want Pending", got)
	}
}

func TestCountVotesStaleTermWhenReplyHasHigherTerm(t *testing.T) {
	replies := []RequestVoteReply{
		{Term: 7, VoteGranted: false},
	}
	if got := CountVotes(4, 3, replies); got != StaleTerm {
		t.Fatalf("CountVotes = %v, want StaleTerm", got)
	}
}

func TestCountVotesIgnoresUngrantedReplies(t *testing.T) {
	replies := []RequestVoteReply{
		{Term: 4, VoteGranted: false},
		{Term: 4, VoteGranted: false},
	}
	if got := CountVotes(4, 3, replies); got != Pending {
		t.Fatalf("CountVotes = %v, want Pending", got)
	}
}

func TestCountVotesSelfVoteAloneWinsSingleNodeCluster(t *testing.T) {
	if got := CountVotes(1, 1, nil); got != Won {
		t.Fatalf("CountVotes = %v, want Won", got)
	}
}

func TestCountVotesMajorityOfFourNeedsThree(t *testing.T) {
	replies := []RequestVoteReply{
		{Term: 4, VoteGranted: true},
	}
	// self + 1 granted = 2, majority of 4 is 3 -> still pending
	if got := CountVotes(4, 4, replies); got != Pending {
		t.Fatalf("CountVotes = %v, want Pending", got)
	}

	replies = append(replies, RequestVoteReply{Term: 4, VoteGranted: true})
	// self + 2 granted = 3, majority of 4 is 3 -> won
	if got := CountVotes(4, 4, replies); got != Won {
		t.Fatalf("CountVotes = %v, want Won", got)
	}
}
