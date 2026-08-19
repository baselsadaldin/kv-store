package raft

import (
	"errors"
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

	// nextIndex and matchIndex are only meaningful while this node is
	// Leader, reset each time it wins an election (see RunElection).
	// nextIndex[peer] is this Leader's guess of the next log index peer
	// needs; matchIndex[peer] is the highest index confirmed (by a
	// successful reply) to actually be on peer's log.
	nextIndex  map[string]int
	matchIndex map[string]int
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

// Propose appends command to the Leader's log as a new entry in its current
// term. It does not wait for replication — the background replication loop
// (driven by Run while this node is Leader) still does the actual work of
// pushing it out to peers and retrying. It returns the index the entry was
// assigned and whether this node is currently the Leader; if it isn't, the
// command is not appended, and the caller (the client-facing handler)
// should redirect elsewhere rather than treat this as accepted.
//
// It also immediately re-checks the commit index: the Leader's own log
// always counts toward a majority, so for a single-node cluster (no peers)
// this is what commits an entry at all, rather than waiting forever for a
// replicatePeer call that has no peer to run against.
func (n *Node) Propose(command Command) (index int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return 0, false
	}
	entry := n.log.Append(n.state.CurrentTerm, command)
	n.advanceCommitIndexLocked()
	n.applyCommittedLocked()
	return entry.Index, true
}

// waitAppliedPollInterval is how often WaitApplied re-checks progress.
const waitAppliedPollInterval = 5 * time.Millisecond

// WaitApplied blocks until the entry at index has been applied to the state
// machine (i.e. lastApplied >= index), or returns an error if timeout
// elapses first, or if this node stops being Leader before that happens —
// in that case the entry may have been discarded by a later Leader (see
// the safety discussion around uncommitted entries), so the caller should
// treat the write as failed rather than assume it went through.
func (n *Node) WaitApplied(index int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		n.mu.Lock()
		applied := n.lastApplied >= index
		stillLeader := n.role == Leader
		n.mu.Unlock()

		if applied {
			return nil
		}
		if !stillLeader {
			return errors.New("no longer leader: entry may not have committed")
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for entry to be applied")
		}
		time.Sleep(waitAppliedPollInterval)
	}
}

// AppendEntriesSender sends an AppendEntries RPC to peer and returns its
// reply. Same injection pattern as RequestVoteSender: a plain function
// rather than a concrete network client, so replication logic is testable
// without real networking; DialAppendEntries is the real implementation.
type AppendEntriesSender func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)

// heartbeatInterval is how often a Leader replicates to (or, if there's
// nothing new, simply heartbeats) each peer. It must be well under
// electionTimeoutMin so Followers reliably hear from a live Leader before
// their own election timeout fires.
const heartbeatInterval = 50 * time.Millisecond

// replicateToAllPeers sends one round of AppendEntries to every peer,
// concurrently, via send. It's a no-op if the node isn't currently Leader.
func (n *Node) replicateToAllPeers(send AppendEntriesSender) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.replicatePeer(peer, send)
		}(peer)
	}
	wg.Wait()
}

// replicatePeer sends one AppendEntries to peer, covering everything from
// nextIndex[peer] through the end of the Leader's log (empty Entries is a
// pure heartbeat if peer is already caught up), and applies the reply:
// stepping down if it reveals a higher term, advancing matchIndex/
// nextIndex and re-checking the commit index on success, or backing
// nextIndex up to retry further back on failure.
func (n *Node) replicatePeer(peer string, send AppendEntriesSender) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	next := max(n.nextIndex[peer], 1)
	prevIndex := next - 1
	prevTerm := 0
	if prevIndex > 0 {
		if e, ok := n.log.Get(prevIndex); ok {
			prevTerm = e.Term
		}
	}
	var entries []Entry
	for i := next; i <= n.log.LastIndex(); i++ {
		if e, ok := n.log.Get(i); ok {
			entries = append(entries, e)
		}
	}
	args := AppendEntriesArgs{
		Term:         n.state.CurrentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := send(peer, args)
	if err != nil {
		return // peer unreachable this round; the next tick will retry
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return // stepped down while this RPC was in flight
	}

	if reply.Term > n.state.CurrentTerm {
		// A peer knows about a later term -- this Leader is stale.
		n.state.CurrentTerm = reply.Term
		n.state.VotedFor = ""
		if n.statePath != "" {
			// Best-effort: even if this fails to persist, stepping down
			// is still correct and safe (it can't cause a double vote).
			SaveState(n.statePath, n.state)
		}
		n.role = Follower
		return
	}

	if reply.Success {
		n.matchIndex[peer] = prevIndex + len(entries)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndexLocked()
		n.applyCommittedLocked()
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
}

// advanceCommitIndexLocked recomputes commitIndex as the highest log index
// present on a majority of the cluster (this Leader included), subject to
// Raft's current-term-only commit rule: an index only counts if the entry
// there was proposed in this Leader's own current term. Entries from
// earlier terms become committed only indirectly, as a side effect of a
// later current-term entry committing (see the Figure-8-style scenario
// this rule closes). The caller must hold n.mu and only call this while
// Leader.
func (n *Node) advanceCommitIndexLocked() {
	matchIndexes := make([]int, 0, len(n.peers)+1)
	matchIndexes = append(matchIndexes, n.log.LastIndex()) // the Leader always has its own entire log
	for _, peer := range n.peers {
		matchIndexes = append(matchIndexes, n.matchIndex[peer])
	}
	majority := len(matchIndexes)/2 + 1

	for idx := n.log.LastIndex(); idx > n.commitIndex; idx-- {
		count := 0
		for _, mi := range matchIndexes {
			if mi >= idx {
				count++
			}
		}
		if count < majority {
			continue
		}
		if entry, ok := n.log.Get(idx); ok && entry.Term == n.state.CurrentTerm {
			n.commitIndex = idx
			return
		}
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
			n.nextIndex = make(map[string]int, len(n.peers))
			n.matchIndex = make(map[string]int, len(n.peers))
			for _, peer := range n.peers {
				// Optimistically assume every peer is fully caught up
				// until an AppendEntries reply proves otherwise.
				n.nextIndex[peer] = n.log.LastIndex() + 1
				n.matchIndex[peer] = 0
			}
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

// Run is the node's background loop, in one of two modes depending on
// role. As Leader, it repeatedly calls replicateToAllPeers every
// heartbeatInterval via sendAppend — this is what actually keeps the
// cluster alive: it's the Leader's heartbeats that call
// ResetElectionTimer on every Follower (via their HandleAppendEntriesRPC),
// which is what stops them from timing out and calling needless elections.
// As Follower or Candidate, it waits for nextTimeout() to elapse without
// being reset, then runs an election via send. It blocks until Stop is
// called, so callers should run it in its own goroutine. nextTimeout is
// normally randomElectionTimeout; tests can pass a faster function.
func (n *Node) Run(send RequestVoteSender, sendAppend AppendEntriesSender, nextTimeout func() time.Duration) {
	for {
		if n.Role() == Leader {
			n.replicateToAllPeers(sendAppend)
			select {
			case <-n.stop:
				return
			case <-time.After(heartbeatInterval):
			}
			continue
		}

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

// RunWithRealTiming starts Run using real, randomized election timeouts —
// the production entry point for a live node. send and sendAppend should
// normally be DialRequestVote and DialAppendEntries. Tests that need fast
// or deterministic timing should call Run directly with a custom
// nextTimeout instead.
func (n *Node) RunWithRealTiming(send RequestVoteSender, sendAppend AppendEntriesSender) {
	n.Run(send, sendAppend, randomElectionTimeout)
}
