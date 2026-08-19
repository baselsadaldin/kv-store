package raft

import (
	"errors"
	"testing"
	"time"
)

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})
	if got := n.Role(); got != Follower {
		t.Fatalf("Role() = %v, want Follower", got)
	}
}

func TestRunElectionBecomesLeaderOnMajority(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})

	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}

	outcome := n.RunElection(send, 20*time.Millisecond)

	if outcome != Won {
		t.Fatalf("outcome = %v, want Won", outcome)
	}
	if got := n.Role(); got != Leader {
		t.Fatalf("Role() = %v, want Leader", got)
	}
}

func TestRunElectionStaysFollowerWithoutMajority(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})

	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{Term: args.Term, VoteGranted: false}, nil
	}

	outcome := n.RunElection(send, 20*time.Millisecond)

	if outcome != Pending {
		t.Fatalf("outcome = %v, want Pending", outcome)
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("Role() = %v, want Follower", got)
	}
}

func TestRunElectionStepsDownOnStaleTerm(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})

	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{Term: args.Term + 5, VoteGranted: false}, nil
	}

	outcome := n.RunElection(send, 20*time.Millisecond)

	if outcome != StaleTerm {
		t.Fatalf("outcome = %v, want StaleTerm", outcome)
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("Role() = %v, want Follower", got)
	}
}

func TestRunElectionIgnoresUnreachablePeers(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})

	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		if peer == "node2" {
			return RequestVoteReply{}, errors.New("connection refused")
		}
		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}

	// self-vote + node3's granted vote = 2, majority of a 3-node cluster
	// is 2, so the election should still succeed despite node2 being
	// unreachable.
	outcome := n.RunElection(send, 50*time.Millisecond)

	if outcome != Won {
		t.Fatalf("outcome = %v, want Won", outcome)
	}
}

func TestRandomElectionTimeoutWithinBounds(t *testing.T) {
	for i := 0; i < 100; i++ {
		got := randomElectionTimeout()
		if got < electionTimeoutMin || got > electionTimeoutMax {
			t.Fatalf("randomElectionTimeout() = %v, want within [%v, %v]", got, electionTimeoutMin, electionTimeoutMax)
		}
	}
}
