package raft

import (
	"errors"
	"path/filepath"
	"sync/atomic"
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

func TestOpenLoadsExistingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	if err := SaveState(path, PersistentState{CurrentTerm: 5, VotedFor: "node9"}); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	n, err := Open("node1", []string{"node2"}, path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if n.state.CurrentTerm != 5 || n.state.VotedFor != "node9" {
		t.Fatalf("got state %+v, want {CurrentTerm: 5, VotedFor: node9}", n.state)
	}
}

func TestOpenWithMissingFileStartsAtZeroState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")

	n, err := Open("node1", []string{"node2"}, path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if n.state != (PersistentState{}) {
		t.Fatalf("got state %+v, want zero value", n.state)
	}
}

func TestHandleRequestVoteRPCPersistsGrantedVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	n, err := Open("node1", []string{"node2"}, path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	reply := n.HandleRequestVoteRPC(RequestVoteArgs{Term: 1, CandidateID: "peer1"})
	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	saved, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if saved.CurrentTerm != 1 || saved.VotedFor != "peer1" {
		t.Fatalf("got persisted state %+v, want {CurrentTerm: 1, VotedFor: peer1}", saved)
	}
}

func TestHandleRequestVoteRPCWorksWithoutStatePath(t *testing.T) {
	n := NewNode("node1", []string{"node2"})

	reply := n.HandleRequestVoteRPC(RequestVoteArgs{Term: 1, CandidateID: "peer1"})
	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted for an in-memory-only node")
	}
}

func TestRunElectionPersistsSelfVoteBeforeReturning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	n, err := Open("node1", []string{"node2", "node3"}, path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}
	n.RunElection(send, 20*time.Millisecond)

	saved, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if saved.CurrentTerm != 1 || saved.VotedFor != "node1" {
		t.Fatalf("got persisted state %+v, want {CurrentTerm: 1, VotedFor: node1}", saved)
	}
}

func TestRunElectionAbortsIfPersistFails(t *testing.T) {
	// The parent directory doesn't exist, so SaveState can never succeed.
	path := filepath.Join(t.TempDir(), "missing-dir", "state")
	n, err := Open("node1", []string{"node2", "node3"}, path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}
	outcome := n.RunElection(send, 20*time.Millisecond)

	if outcome != Pending {
		t.Fatalf("outcome = %v, want Pending", outcome)
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("Role() = %v, want Follower", got)
	}
}

func TestHandleRequestVoteRPCStepsDownFromLeaderOnHigherTerm(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})
	n.role = Leader
	n.state = PersistentState{CurrentTerm: 3}

	reply := n.HandleRequestVoteRPC(RequestVoteArgs{Term: 4, CandidateID: "node2"})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("Role() = %v, want Follower after seeing a higher term", got)
	}
}

func fastTimeout(d time.Duration) func() time.Duration {
	return func() time.Duration { return d }
}

func TestRunTriggersElectionOnTimeout(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})
	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}

	go n.Run(send, fastTimeout(10*time.Millisecond))
	defer n.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n.Role() == Leader {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected the node to become Leader via the election timer, but it never did")
}

func TestResetElectionTimerPreventsElection(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})

	var sent int32
	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		atomic.AddInt32(&sent, 1)
		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}

	go n.Run(send, fastTimeout(20*time.Millisecond))
	defer n.Stop()

	// Keep resetting faster than the timeout for a while; no election
	// should ever get a chance to fire.
	resetDeadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(resetDeadline) {
		n.ResetElectionTimer()
		time.Sleep(5 * time.Millisecond)
	}

	if got := atomic.LoadInt32(&sent); got != 0 {
		t.Fatalf("expected no election to run while resets kept arriving, but send was called %d time(s)", got)
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("Role() = %v, want Follower", got)
	}
}

func TestRunDoesNotRestartElectionWhileLeader(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})
	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
	}

	go n.Run(send, fastTimeout(10*time.Millisecond))
	defer n.Stop()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) && n.Role() != Leader {
		time.Sleep(5 * time.Millisecond)
	}
	if n.Role() != Leader {
		t.Fatal("node never became Leader")
	}

	// Give the timer several more cycles to fire; role should stay Leader
	// instead of re-electing itself away from it.
	time.Sleep(100 * time.Millisecond)
	if got := n.Role(); got != Leader {
		t.Fatalf("Role() = %v, want Leader (should not re-trigger elections while already Leader)", got)
	}
}

func TestStopEndsRunLoop(t *testing.T) {
	n := NewNode("node1", nil)
	send := func(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
		return RequestVoteReply{}, nil
	}

	done := make(chan struct{})
	go func() {
		n.Run(send, fastTimeout(10*time.Millisecond))
		close(done)
	}()

	n.Stop()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not return after Stop was called")
	}
}
