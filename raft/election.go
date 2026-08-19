package raft

// RequestVoteArgs is sent by a Candidate asking peers to vote for it.
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is a peer's response to a RequestVoteArgs.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// HandleRequestVote decides how to respond to a RequestVote RPC, given the
// receiver's current persistent state and log. It returns the (possibly
// updated) state alongside the reply. The caller must durably save that
// state via SaveState BEFORE actually sending the reply, so a crash between
// deciding and replying can never cause the node to forget a vote it
// already promised (see PersistentState).
func HandleRequestVote(state PersistentState, log *Log, args RequestVoteArgs) (PersistentState, RequestVoteReply) {
	if args.Term > state.CurrentTerm {
		state.CurrentTerm = args.Term
		state.VotedFor = ""
	}

	if args.Term < state.CurrentTerm {
		return state, RequestVoteReply{Term: state.CurrentTerm, VoteGranted: false}
	}

	alreadyVotedForSomeoneElse := state.VotedFor != "" && state.VotedFor != args.CandidateID
	logUpToDate := isLogUpToDate(args.LastLogTerm, args.LastLogIndex, log.LastTerm(), log.LastIndex())

	if alreadyVotedForSomeoneElse || !logUpToDate {
		return state, RequestVoteReply{Term: state.CurrentTerm, VoteGranted: false}
	}

	state.VotedFor = args.CandidateID
	return state, RequestVoteReply{Term: state.CurrentTerm, VoteGranted: true}
}

// isLogUpToDate reports whether a candidate's log (identified by the term
// and index of its last entry) is at least as up-to-date as the voter's own
// log, per Raft's election restriction: the log with the later last-entry
// term wins outright; if those terms are equal, the longer log wins.
func isLogUpToDate(candidateLastTerm, candidateLastIndex, voterLastTerm, voterLastIndex int) bool {
	if candidateLastTerm != voterLastTerm {
		return candidateLastTerm > voterLastTerm
	}
	return candidateLastIndex >= voterLastIndex
}
