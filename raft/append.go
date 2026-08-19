package raft

// AppendEntriesArgs is sent by a Leader to replicate log entries to a
// Follower, or as a heartbeat when Entries is empty.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// AppendEntriesReply is a Follower's response to an AppendEntriesArgs.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// HandleAppendEntries decides how to respond to an AppendEntries RPC, given
// the receiver's current persistent state and log. Unlike HandleRequestVote,
// it mutates log directly (it's a pointer) when the request passes the
// consistency check, since applying the replication IS the decision here.
// It returns the (possibly updated) state alongside the reply.
//
// Advancing the receiver's own commit index from args.LeaderCommit is
// handled separately, not here — that's the next step, once this piece is
// solid on its own.
func HandleAppendEntries(state PersistentState, log *Log, args AppendEntriesArgs) (PersistentState, AppendEntriesReply) {
	if args.Term > state.CurrentTerm {
		state.CurrentTerm = args.Term
		state.VotedFor = ""
	}

	if args.Term < state.CurrentTerm {
		return state, AppendEntriesReply{Term: state.CurrentTerm, Success: false}
	}

	// PrevLogIndex == 0 is the sentinel for "no previous entry required"
	// — replicating from the very start of the log — which always
	// trivially matches.
	if args.PrevLogIndex > 0 {
		prevEntry, ok := log.Get(args.PrevLogIndex)
		if !ok || prevEntry.Term != args.PrevLogTerm {
			return state, AppendEntriesReply{Term: state.CurrentTerm, Success: false}
		}
	}

	for _, entry := range args.Entries {
		existing, ok := log.Get(entry.Index)
		if ok && existing.Term != entry.Term {
			// A conflicting entry from a stale term is sitting here —
			// discard it and everything after before appending the
			// Leader's version.
			log.TruncateFrom(entry.Index)
			ok = false
		}
		if !ok {
			log.Append(entry.Term, entry.Command)
		}
		// If ok is still true here, this exact entry is already present
		// (a retried RPC) — nothing to do.
	}

	return state, AppendEntriesReply{Term: state.CurrentTerm, Success: true}
}
