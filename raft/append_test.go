package raft

import "testing"

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	state := PersistentState{CurrentTerm: 5}
	log := NewLog()

	newState, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term: 3, LeaderID: "leader1",
	})

	if reply.Success {
		t.Fatal("expected rejection for a stale term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
	if newState != state {
		t.Fatalf("state changed on a rejected stale-term request: got %+v, want unchanged %+v", newState, state)
	}
}

func TestAppendEntriesAdoptsHigherTermAndResetsVote(t *testing.T) {
	state := PersistentState{CurrentTerm: 1, VotedFor: "node2"}
	log := NewLog()

	newState, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term: 3, LeaderID: "leader1",
	})

	if !reply.Success {
		t.Fatal("expected success: higher term with an empty log and PrevLogIndex 0 always matches")
	}
	if newState.CurrentTerm != 3 {
		t.Fatalf("CurrentTerm = %d, want 3", newState.CurrentTerm)
	}
	if newState.VotedFor != "" {
		t.Fatalf("VotedFor = %q, want empty after adopting a new term", newState.VotedFor)
	}
}

func TestAppendEntriesAcceptsFirstEntryWithZeroPrevLogIndex(t *testing.T) {
	state := PersistentState{CurrentTerm: 1}
	log := NewLog()

	_, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term:         1,
		PrevLogIndex: 0,
		Entries:      []Entry{{Term: 1, Index: 1, Command: Command{Op: "SET", Key: "a", Value: "1"}}},
	})

	if !reply.Success {
		t.Fatal("expected success appending the first entry to an empty log")
	}
	if got := log.LastIndex(); got != 1 {
		t.Fatalf("LastIndex() = %d, want 1", got)
	}
}

func TestAppendEntriesRejectsWhenPrevLogIndexMissing(t *testing.T) {
	state := PersistentState{CurrentTerm: 1}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})

	_, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term: 1, PrevLogIndex: 5, PrevLogTerm: 1,
	})

	if reply.Success {
		t.Fatal("expected rejection: PrevLogIndex 5 doesn't exist in a 1-entry log")
	}
}

func TestAppendEntriesRejectsWhenPrevLogTermMismatch(t *testing.T) {
	state := PersistentState{CurrentTerm: 2}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"}) // index 1, term 1

	_, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term: 2, PrevLogIndex: 1, PrevLogTerm: 2, // term mismatch: log has term 1 there
	})

	if reply.Success {
		t.Fatal("expected rejection: PrevLogTerm doesn't match the entry actually at PrevLogIndex")
	}
}

func TestAppendEntriesAppendsNewEntriesInOrder(t *testing.T) {
	state := PersistentState{CurrentTerm: 1}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"}) // index 1

	_, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term: 1, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{
			{Term: 1, Index: 2, Command: Command{Op: "SET", Key: "b", Value: "2"}},
			{Term: 1, Index: 3, Command: Command{Op: "DEL", Key: "a"}},
		},
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if got := log.LastIndex(); got != 3 {
		t.Fatalf("LastIndex() = %d, want 3", got)
	}
	e2, _ := log.Get(2)
	if e2.Command.Key != "b" {
		t.Fatalf("entry 2 key = %q, want %q", e2.Command.Key, "b")
	}
	e3, _ := log.Get(3)
	if e3.Command.Op != "DEL" {
		t.Fatalf("entry 3 op = %q, want DEL", e3.Command.Op)
	}
}

func TestAppendEntriesTruncatesConflictingEntries(t *testing.T) {
	state := PersistentState{CurrentTerm: 4}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})     // index 1, term 1
	log.Append(3, Command{Op: "SET", Key: "stale", Value: "x"}) // index 2, term 3 (from an abandoned leader)
	log.Append(3, Command{Op: "SET", Key: "gone", Value: "y"})  // index 3, term 3

	_, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term: 4, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{
			{Term: 4, Index: 2, Command: Command{Op: "SET", Key: "real", Value: "z"}},
		},
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if got := log.LastIndex(); got != 2 {
		t.Fatalf("LastIndex() = %d, want 2 (conflicting tail should be truncated)", got)
	}
	e2, _ := log.Get(2)
	if e2.Command.Key != "real" || e2.Term != 4 {
		t.Fatalf("entry 2 = %+v, want the new Leader's entry (term 4, key real)", e2)
	}
}

func TestAppendEntriesIsIdempotentOnRetry(t *testing.T) {
	state := PersistentState{CurrentTerm: 1}
	log := NewLog()

	args := AppendEntriesArgs{
		Term: 1, PrevLogIndex: 0,
		Entries: []Entry{{Term: 1, Index: 1, Command: Command{Op: "SET", Key: "a", Value: "1"}}},
	}

	state, reply1 := HandleAppendEntries(state, log, args)
	if !reply1.Success {
		t.Fatal("expected first call to succeed")
	}
	_, reply2 := HandleAppendEntries(state, log, args)
	if !reply2.Success {
		t.Fatal("expected a retried, identical call to also succeed")
	}
	if got := log.LastIndex(); got != 1 {
		t.Fatalf("LastIndex() = %d, want 1 (retry should not duplicate the entry)", got)
	}
}

func TestAppendEntriesHeartbeatWithNoEntriesSucceeds(t *testing.T) {
	state := PersistentState{CurrentTerm: 1}
	log := NewLog()
	log.Append(1, Command{Op: "SET", Key: "a", Value: "1"})

	_, reply := HandleAppendEntries(state, log, AppendEntriesArgs{
		Term: 1, PrevLogIndex: 1, PrevLogTerm: 1, Entries: nil,
	})

	if !reply.Success {
		t.Fatal("expected a heartbeat (no entries) to succeed once the consistency check passes")
	}
	if got := log.LastIndex(); got != 1 {
		t.Fatalf("LastIndex() = %d, want 1 (heartbeat should not change the log)", got)
	}
}
