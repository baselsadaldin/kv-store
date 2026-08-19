package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Role is the three states a Raft node can be in.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// electionTimeoutMin and electionTimeoutMax bound the randomized duration a
// node waits without hearing from a Leader before starting an election.
// Randomizing spreads timeouts across nodes so a single Leader crash
// doesn't make every Follower become a Candidate at once and split the
// vote.
const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

// Node holds one Raft node's role and state and is safe for concurrent use.
// It has no networking of its own yet (see RequestVoteSender) and no
// background timer loop yet — those are still to come.
type Node struct {
	mu    sync.Mutex
	id    string
	peers []string
	role  Role
	state PersistentState
	log   *Log
}

// NewNode creates a Node starting as a Follower with an empty log and zero
// persistent state.
func NewNode(id string, peers []string) *Node {
	return &Node{id: id, peers: peers, role: Follower, log: NewLog()}
}

// Role reports the node's current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// RequestVoteSender sends a RequestVote RPC to peer and returns its reply.
// It's a function type rather than a concrete network client so election
// logic can be tested without real networking; the real TCP implementation
// is a later step.
type RequestVoteSender func(peer string, args RequestVoteArgs) (RequestVoteReply, error)

// RunElection drives one full election attempt: transitions the node to
// Candidate, starts a new term (via StartElection), sends RequestVote to
// every peer concurrently via send, waits up to timeout for replies,
// tallies them (via CountVotes), and updates the node's role accordingly —
// Leader on a majority, Follower otherwise (including on a stale-term
// reply). It returns the outcome.
func (n *Node) RunElection(send RequestVoteSender, timeout time.Duration) VoteOutcome {
	n.mu.Lock()
	n.role = Candidate
	newState, args := StartElection(n.state, n.log, n.id)
	n.state = newState
	currentTerm := n.state.CurrentTerm
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	replies := make(chan RequestVoteReply, len(peers))
	for _, peer := range peers {
		go func() {
			if reply, err := send(peer, args); err == nil {
				replies <- reply
			}
		}()
	}

	collected := make([]RequestVoteReply, 0, len(peers))
	deadline := time.After(timeout)
collectLoop:
	for range peers {
		select {
		case r := <-replies:
			collected = append(collected, r)
		case <-deadline:
			break collectLoop
		}
	}

	outcome := CountVotes(currentTerm, len(peers)+1, collected)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Candidate {
		if outcome == Won {
			n.role = Leader
		} else {
			n.role = Follower
		}
	}
	return outcome
}
