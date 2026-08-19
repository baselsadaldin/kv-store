package raft

// StartElection transitions a node into a Candidate for a new term: it
// bumps CurrentTerm, votes for itself, and returns the RequestVoteArgs
// ready to send to every peer. The caller must persist the returned state
// via SaveState BEFORE actually sending any RPCs, since it already reflects
// a self-vote (see PersistentState).
func StartElection(state PersistentState, log *Log, selfID string) (PersistentState, RequestVoteArgs) {
	state.CurrentTerm++
	state.VotedFor = selfID

	args := RequestVoteArgs{
		Term:         state.CurrentTerm,
		CandidateID:  selfID,
		LastLogIndex: log.LastIndex(),
		LastLogTerm:  log.LastTerm(),
	}
	return state, args
}

// VoteOutcome is the result of tallying RequestVote replies collected so
// far during an election.
type VoteOutcome int

const (
	// Pending means neither a majority nor a higher term has been seen
	// yet; the candidate should keep waiting for more replies (or time
	// out and start a new election).
	Pending VoteOutcome = iota
	// Won means a majority of the cluster (including the candidate's own
	// self-vote) has granted its vote.
	Won
	// StaleTerm means a reply revealed a term higher than the one this
	// election was fought in; the candidate must abandon its candidacy
	// and revert to Follower.
	StaleTerm
)

// CountVotes tallies replies collected so far for an election fought in
// currentTerm, against a cluster of clusterSize nodes (the candidate
// itself included), and returns the outcome. The candidate's own self-vote
// (cast in StartElection) is counted automatically and should not be
// included in replies.
func CountVotes(currentTerm, clusterSize int, replies []RequestVoteReply) VoteOutcome {
	for _, r := range replies {
		if r.Term > currentTerm {
			return StaleTerm
		}
	}

	granted := 1 // the candidate's own self-vote
	for _, r := range replies {
		if r.VoteGranted {
			granted++
		}
	}

	majority := clusterSize/2 + 1
	if granted >= majority {
		return Won
	}
	return Pending
}
