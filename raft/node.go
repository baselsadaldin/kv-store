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
type Node struct {
	mu          sync.Mutex
	id          string
	peers       []string
	role        Role
	state       PersistentState
	log         *Log
	statePath   string // empty means "in-memory only, don't persist" (see NewNode vs Open)
	resetTimer  chan struct{}
	stop        chan struct{}
	commitIndex int // highest log index known to be committed; volatile, rebuilt on restart, not persisted
	lastApplied int // highest log index applied to the state machine via apply; volatile, always <= commitIndex
	apply       Applier
}

// Applier applies a committed command to the underlying state machine —
// normally store.Set/store.Delete. It's injected rather than Node holding a
// concrete *store.Store directly, so Raft logic stays testable without a
// real store; the real wiring to store.Store happens where a Node is
// constructed for actual use.
type Applier func(Command) error

// SetApplier registers the function used to apply newly committed log
// entries to the state machine (see applyCommittedLocked). Until this is
// called, commitIndex still advances correctly, but nothing gets applied —
// fine for tests that only care about consensus, not storage.
func (n *Node) SetApplier(apply Applier) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.apply = apply
}

// NewNode creates a Node starting as a Follower with an empty log and zero
// persistent state, held in memory only — nothing is ever saved to disk.
// This is meant for tests that only care about decision logic; a real,
// running node should use Open instead so votes and term changes survive a
// crash.
func NewNode(id string, peers []string) *Node {
	return &Node{id: id, peers: peers, role: Follower, log: NewLog(), resetTimer: make(chan struct{}, 1), stop: make(chan struct{})}
}

// Open loads (or, on first run, initializes) a Node's persistent state from
// statePath, mirroring store.Open's role for the storage engine. The
// returned Node persists its term and vote to statePath from then on,
// before ever acting on them over the network.
func Open(id string, peers []string, statePath string) (*Node, error) {
	state, err := LoadState(statePath)
	if err != nil {
		return nil, err
	}
	return &Node{id: id, peers: peers, role: Follower, log: NewLog(), state: state, statePath: statePath, resetTimer: make(chan struct{}, 1), stop: make(chan struct{})}, nil
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

// HandleRequestVoteRPC applies an incoming RequestVote RPC to the node: it
// runs the decision logic in HandleRequestVote under the node's lock and,
// for a node opened with Open, durably saves any resulting state change
// BEFORE returning — so the reply can be trusted even if the node crashes
// immediately after sending it (see PersistentState). If the request
// reveals a higher term than the node knew about, the node also steps down
// to Follower regardless of its previous role — a Leader or Candidate that
// hasn't heard about a newer term yet must not keep acting like one once it
// has. Its signature matches RequestVoteHandler, so it can be passed
// directly to ListenAndServe, e.g. ListenAndServe(addr, node.HandleRequestVoteRPC).
func (n *Node) HandleRequestVoteRPC(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	newState, reply := HandleRequestVote(n.state, n.log, args)
	if newState.CurrentTerm > n.state.CurrentTerm {
		n.role = Follower
	}
	if newState != n.state && n.statePath != "" {
		if err := SaveState(n.statePath, newState); err != nil {
			// Couldn't durably save the term bump or cast vote -- refuse
			// rather than risk the node forgetting it after a crash.
			return RequestVoteReply{Term: n.state.CurrentTerm, VoteGranted: false}
		}
	}
	n.state = newState
	return reply
}

// HandleAppendEntriesRPC applies an incoming AppendEntries RPC to the node:
// it runs the decision logic in HandleAppendEntries (which mutates n.log
// directly) under the node's lock, and for a node opened with Open, durably
// saves any resulting state change BEFORE returning.
//
// Unlike a RequestVote reply, any AppendEntries carrying a term at least as
// current as this node's own is treated as proof of a legitimate Leader —
// even if the consistency check fails and Success comes back false (that
// just means the logs need reconciling, not that the sender isn't really
// the Leader). So on any non-stale request, the node steps down to Follower
// (a Candidate must stop competing once it learns someone else already won)
// and resets the election timer via ResetElectionTimer, so Run's background
// loop knows a real Leader is alive and doesn't call a needless election.
//
// Only once the request succeeds does the node advance its own commitIndex
// toward args.LeaderCommit, capped at how much log it actually has.
func (n *Node) HandleAppendEntriesRPC(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	newState, reply := HandleAppendEntries(n.state, n.log, args)

	if args.Term >= n.state.CurrentTerm {
		n.role = Follower
		n.ResetElectionTimer()
	}

	if newState != n.state && n.statePath != "" {
		if err := SaveState(n.statePath, newState); err != nil {
			return AppendEntriesReply{Term: n.state.CurrentTerm, Success: false}
		}
	}
	n.state = newState

	if reply.Success {
		if args.LeaderCommit > n.commitIndex {
			n.commitIndex = args.LeaderCommit
			if n.commitIndex > n.log.LastIndex() {
				n.commitIndex = n.log.LastIndex()
			}
		}
		n.applyCommittedLocked()
	}

	return reply
}

// applyCommittedLocked applies every log entry between lastApplied and
// commitIndex, in order, via the injected Applier. The caller must already
// hold n.mu. It's a no-op if no Applier has been set (see SetApplier). If
// applying an entry fails, it stops there without advancing lastApplied
// past it, so the same entry gets retried the next time this runs (e.g. on
// the next heartbeat) rather than being silently skipped.
func (n *Node) applyCommittedLocked() {
	if n.apply == nil {
		return
	}
	for n.lastApplied < n.commitIndex {
		next := n.lastApplied + 1
		entry, ok := n.log.Get(next)
		if !ok {
			return // shouldn't happen if commitIndex was capped correctly
		}
		if err := n.apply(entry.Command); err != nil {
			return
		}
		n.lastApplied = next
	}
}

// RunElection drives one full election attempt: transitions the node to
// Candidate, starts a new term (via StartElection), persists the resulting
// self-vote (for a node opened with Open), sends RequestVote to every peer
// concurrently via send, waits up to timeout for replies, tallies them (via
// CountVotes), and updates the node's role accordingly — Leader on a
// majority, Follower otherwise (including on a stale-term reply, or if the
// self-vote couldn't be durably saved). It returns the outcome.
func (n *Node) RunElection(send RequestVoteSender, timeout time.Duration) VoteOutcome {
	n.mu.Lock()
	n.role = Candidate
	newState, args := StartElection(n.state, n.log, n.id)
	if n.statePath != "" {
		if err := SaveState(n.statePath, newState); err != nil {
			// Can't safely become a Candidate if the self-vote can't be
			// persisted -- abort back to Follower rather than risk
			// forgetting it after a crash.
			n.role = Follower
			n.mu.Unlock()
			return Pending
		}
	}
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

// ResetElectionTimer signals the background loop started by Run that a
// legitimate heartbeat was received, so it restarts its timeout window
// instead of starting a new election. It's safe to call whether or not Run
// is currently active — a signal sent when nothing is listening is simply
// dropped rather than blocking.
func (n *Node) ResetElectionTimer() {
	select {
	case n.resetTimer <- struct{}{}:
	default:
	}
}

// Stop ends the background loop started by Run.
func (n *Node) Stop() {
	close(n.stop)
}

// Run is the node's background election timer loop: it waits for
// nextTimeout() to elapse without being reset (via ResetElectionTimer),
// then — unless the node is currently Leader — runs an election via send.
// It blocks until Stop is called, so callers should run it in its own
// goroutine. nextTimeout is normally randomElectionTimeout; tests can pass
// a faster function to avoid waiting on real timeouts.
//
// A Leader deliberately never re-triggers its own election here: real Raft
// has a Leader actively sending heartbeats on its own separate ticker
// instead, which resets every Follower's timer — that heartbeat-sending
// side doesn't exist yet (it's part of AppendEntries), so for now a Leader
// simply idles rather than needlessly demoting itself.
func (n *Node) Run(send RequestVoteSender, nextTimeout func() time.Duration) {
	for {
		timer := time.NewTimer(nextTimeout())
		select {
		case <-n.stop:
			timer.Stop()
			return
		case <-n.resetTimer:
			timer.Stop()
		case <-timer.C:
			if n.Role() != Leader {
				n.RunElection(send, electionTimeoutMax)
			}
		}
	}
}
