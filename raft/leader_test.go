package raft

import (
	"errors"
	"testing"
)

func TestProposeAppendsToLeaderLog(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})
	n.role = Leader
	n.state = PersistentState{CurrentTerm: 2}

	index, isLeader := n.Propose(Command{Op: "SET", Key: "foo", Value: "bar"})

	if !isLeader {
		t.Fatal("expected Propose to succeed while Leader")
	}
	if index != 1 {
		t.Fatalf("index = %d, want 1", index)
	}
	entry, ok := n.log.Get(1)
	if !ok || entry.Term != 2 || entry.Command.Key != "foo" {
		t.Fatalf("got entry %+v, ok=%v, want term 2 command foo", entry, ok)
	}
}

func TestProposeRejectsWhenNotLeader(t *testing.T) {
	n := NewNode("node1", []string{"node2", "node3"})

	_, isLeader := n.Propose(Command{Op: "SET", Key: "foo", Value: "bar"})

	if isLeader {
		t.Fatal("expected Propose to reject a non-Leader node")
	}
	if got := n.log.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0 (nothing should be appended)", got)
	}
}

func newTestLeader(id string, peers []string, term int) *Node {
	n := NewNode(id, peers)
	n.role = Leader
	n.state = PersistentState{CurrentTerm: term}
	n.nextIndex = make(map[string]int, len(peers))
	n.matchIndex = make(map[string]int, len(peers))
	for _, p := range peers {
		n.nextIndex[p] = n.log.LastIndex() + 1
		n.matchIndex[p] = 0
	}
	return n
}

func TestReplicatePeerSendsCorrectPrevLogAndEntries(t *testing.T) {
	n := newTestLeader("node1", []string{"node2"}, 3)
	n.log.Append(3, Command{Op: "SET", Key: "a", Value: "1"}) // index 1
	n.log.Append(3, Command{Op: "SET", Key: "b", Value: "2"}) // index 2
	n.nextIndex["node2"] = 1                                  // node2 hasn't gotten anything yet

	var captured AppendEntriesArgs
	send := func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
		captured = args
		return AppendEntriesReply{Term: 3, Success: true}, nil
	}

	n.replicatePeer("node2", send)

	if captured.PrevLogIndex != 0 {
		t.Fatalf("PrevLogIndex = %d, want 0", captured.PrevLogIndex)
	}
	if len(captured.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(captured.Entries))
	}
}

func TestReplicatePeerAdvancesMatchIndexOnSuccess(t *testing.T) {
	n := newTestLeader("node1", []string{"node2"}, 1)
	n.log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})

	send := func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
		return AppendEntriesReply{Term: 1, Success: true}, nil
	}

	n.replicatePeer("node2", send)

	if n.matchIndex["node2"] != 1 {
		t.Fatalf("matchIndex[node2] = %d, want 1", n.matchIndex["node2"])
	}
	if n.nextIndex["node2"] != 2 {
		t.Fatalf("nextIndex[node2] = %d, want 2", n.nextIndex["node2"])
	}
}

func TestReplicatePeerBacksUpNextIndexOnFailure(t *testing.T) {
	n := newTestLeader("node1", []string{"node2"}, 1)
	n.log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	n.log.Append(1, Command{Op: "SET", Key: "b", Value: "2"})
	n.nextIndex["node2"] = 3

	send := func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
		return AppendEntriesReply{Term: 1, Success: false}, nil
	}

	n.replicatePeer("node2", send)

	if n.nextIndex["node2"] != 2 {
		t.Fatalf("nextIndex[node2] = %d, want 2 (should back up by one on failure)", n.nextIndex["node2"])
	}
}

func TestReplicatePeerStepsDownOnHigherTermReply(t *testing.T) {
	n := newTestLeader("node1", []string{"node2"}, 1)

	send := func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
		return AppendEntriesReply{Term: 5, Success: false}, nil
	}

	n.replicatePeer("node2", send)

	if got := n.Role(); got != Follower {
		t.Fatalf("Role() = %v, want Follower after a higher-term reply", got)
	}
	if n.state.CurrentTerm != 5 {
		t.Fatalf("CurrentTerm = %d, want 5", n.state.CurrentTerm)
	}
}

func TestReplicatePeerIgnoresUnreachablePeer(t *testing.T) {
	n := newTestLeader("node1", []string{"node2"}, 1)

	send := func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
		return AppendEntriesReply{}, errors.New("connection refused")
	}

	n.replicatePeer("node2", send) // must not panic

	if got := n.Role(); got != Leader {
		t.Fatalf("Role() = %v, want unchanged Leader", got)
	}
}

// TestAdvanceCommitIndexRequiresCurrentTermEntry is the Figure-8-style
// safety rule: a Leader must not commit an entry from an earlier term
// purely because a majority happens to have replicated it -- only once a
// majority has replicated an entry from the Leader's OWN current term does
// that (and everything before it) become safely committed.
func TestAdvanceCommitIndexRequiresCurrentTermEntry(t *testing.T) {
	n := newTestLeader("node1", []string{"node2", "node3"}, 4)
	n.log.Append(2, Command{Op: "SET", Key: "old", Value: "stale-term-entry"}) // index 1, term 2

	// A majority (node1 + node2) already has this old-term entry.
	n.matchIndex["node2"] = 1
	n.matchIndex["node3"] = 0

	n.advanceCommitIndexLocked()

	if n.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0 (must not commit an old-term entry on majority alone)", n.commitIndex)
	}

	// Now the Leader replicates something from its OWN current term (4) to
	// the same majority.
	n.log.Append(4, Command{Op: "SET", Key: "new", Value: "current-term-entry"}) // index 2, term 4
	n.matchIndex["node2"] = 2

	n.advanceCommitIndexLocked()

	if n.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2 (a current-term entry on a majority commits it AND everything before it)", n.commitIndex)
	}
}

func TestAdvanceCommitIndexNeedsActualMajority(t *testing.T) {
	n := newTestLeader("node1", []string{"node2", "node3", "node4"}, 1)
	n.log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})

	// Only node2 has it besides the Leader itself: 2 out of 4, not a
	// majority (needs 3).
	n.matchIndex["node2"] = 1
	n.matchIndex["node3"] = 0
	n.matchIndex["node4"] = 0

	n.advanceCommitIndexLocked()

	if n.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0 (2 of 4 is not a majority)", n.commitIndex)
	}
}
