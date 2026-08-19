# Project: Distributed Key-Value Store

## Overview

A distributed key-value store written in Go, built as a high-impact portfolio project
to demonstrate systems programming and distributed systems fundamentals.

## Goals

- Showcase backend/systems engineering skills for job applications (junior SWE /
  verification roles)
- Build toward a production-grade design: durability, consistency, and fault tolerance

## Roadmap (4 stages)

1. **Single-node implementation** — core storage engine, basic get/set/delete API,
   persistence to disk
2. **Networking & client-server layer** — expose the store over the network,
   handle concurrent clients
3. **Raft-based consensus** — replicate state across multiple nodes, leader
   election, log replication for fault tolerance
4. **Production polish** — observability (metrics/logging), benchmarking,
   documentation, deployment tooling

## Status

Stages 1-3 complete: the store is a real, runnable Raft cluster. Stage 4
(observability, benchmarking, deployment polish) hasn't started.

Stage 1: in-memory store with a write-ahead log for persistence: durable
Set/Delete (fsync on every write), crash recovery from torn writes on
reopen, and log compaction (manual via `Compact`, or automatic once the log
holds ≥100 records and ≥4x the live key count) to bound log growth.

Stage 2: the store is now served over TCP instead of embedded in a single
process. `cmd/kvstore` is the server (`--addr`, optionally `--file` for WAL
persistence); `cmd/kvcli` is a separate REPL client that connects to it.
Server handles each connection on its own goroutine against one shared
`*store.Store`. See `server/server.go` for the line protocol
(SET/GET/DELETE/KEYS/COMPACT in, OK/VALUE/NIL/ERR/KEY.../END out).

Stage 3: Raft consensus lives in a new `raft` package, built incrementally
and deliberately kept independent of networking at first so each piece was
unit-testable in isolation before any TCP was involved.

- **Persistence** (`state.go`): `PersistentState` (durable
  `currentTerm`/`votedFor`), written via the same write-temp-file +
  atomic-rename pattern as `store`'s log compaction.
- **Log** (`log.go`): an in-memory, 1-indexed `Log` of term-tagged entries
  (`Append`/`Get`/`LastIndex`/`LastTerm`/`TruncateFrom`, the last of which
  implements Raft's conflicting-entry resolution).
- **Elections** (`election.go`, `candidate.go`): `HandleRequestVote` (the
  receiving side — term/vote/log-up-to-date checks) and
  `StartElection`/`CountVotes` (the candidate side — becoming a candidate,
  tallying majority/stale-term outcomes), all pure, no-I/O functions.
- **Replication** (`append.go`): `HandleAppendEntries` (the receiving
  side — term check, `PrevLogIndex`/`PrevLogTerm` consistency check,
  conflict truncation via `Log.TruncateFrom`, idempotent on retry).
- **Transport** (`transport.go`): `ListenAndServe`/`DialRequestVote`/
  `DialAppendEntries` — one real TCP listener per node handling both RPC
  kinds on a single port (each connection leads with a one-byte type tag,
  then one `encoding/gob`-encoded request and reply).
- **`Node`** (`node.go`, `leader.go`-equivalent logic inside `node.go`)
  ties all of the above into a running node:
  - `Open`/`HandleRequestVoteRPC`/`HandleAppendEntriesRPC` persist state
    to disk *before* granting a vote, casting a self-vote, or acking a
    term change — a crash can never cause a double vote in one term.
  - `Run` is the background loop: as Follower/Candidate, it waits for a
    randomized election timeout and calls `RunElection`
    (`RequestVoteSender`-injected) unless reset via `ResetElectionTimer`
    (which `HandleAppendEntriesRPC` now calls for real on any non-stale
    AppendEntries); as Leader, it calls `replicateToAllPeers` every
    `heartbeatInterval` instead.
  - `replicatePeer` sends each peer an `AppendEntries` covering
    `nextIndex[peer]` onward (`AppendEntriesSender`-injected), advances
    `nextIndex`/`matchIndex` on success or backs `nextIndex` up to retry
    on failure, and steps down to Follower on any higher-term reply.
  - `advanceCommitIndexLocked` recomputes `commitIndex` as the highest
    index on a majority — enforcing Raft's current-term-only commit rule
    (an old-term entry only commits indirectly, once a current-term entry
    reaches majority alongside it; see the Figure-8 scenario this closes).
  - `SetApplier`/`applyCommittedLocked` walk `lastApplied` up to
    `commitIndex`, calling an injected `Applier` (normally
    `store.Set`/`store.Delete`, kept decoupled the same way
    `RequestVoteSender` decouples elections from real networking),
    retrying an entry on failure rather than skipping it.
  - `Propose` appends a new command to the Leader's own log; the running
    replication loop picks it up and pushes it out from there.
  - `WaitApplied` blocks a caller until a given log index has been applied
    (or errors out on timeout or on losing leadership before that
    happens); `Propose` also re-checks the commit index immediately after
    appending, since the Leader's own log always counts toward a majority
    on its own — otherwise a single-node cluster (no peers) would never
    commit anything, since nothing would ever call the peer-driven commit
    check in `replicatePeer`.
- **Proof the algorithm actually works together**:
  `TestThreeNodeClusterElectsLeaderReplicatesAndApplies`
  (`cluster_test.go`) spins up three real `Node`s with real `Transport`s
  on loopback TCP, lets them elect a Leader purely through the background
  timer loop (no test code drives the protocol by hand), `Propose`s a
  write on whichever node won, and asserts it gets replicated, committed
  by majority, and applied on all three nodes' own state machines.

**Wired into `cmd/kvstore` for real use.** New flags: `--raft-addr`
(this node's Raft RPC address — also doubles as its Raft node ID),
`--raft-peers` (comma-separated `--raft-addr` values of the other nodes),
`--raft-state` (where this node's term/vote persist). When `--raft-addr`
is set, `main` opens a `raft.Node`, wires its `Applier` to
`store.Set`/`store.Delete`, starts its `Transport`, runs it via
`RunWithRealTiming`, and constructs the client-facing `server.Server` with
`server.NewRaft` instead of `server.New` — which routes `SET`/`DELETE`
through `Node.Propose` + `WaitApplied` (blocking until committed and
applied, or erroring — including a plain "not the leader" error on a
non-Leader node, since only the Leader accepts writes) instead of writing
to `store` directly. `GET`/`KEYS`/`COMPACT` are unchanged either way —
they always read/act on that node's own local store, so a Follower that
hasn't caught up yet can return stale data for those; only `SET`/`DELETE`
carry Raft's consistency guarantee. Manually verified end-to-end: a real
3-node cluster elects a Leader, replicates a write to all three, and after
killing the Leader process outright, the survivors elect a new Leader
within a couple of election-timeout cycles and continue accepting writes
without losing prior data.

Still open, and each is a reasonable follow-up rather than a gap in the
core algorithm: Raft log compaction/snapshotting (`Log` keeps every entry
forever — no counterpart yet to `store`'s WAL compaction); dynamic cluster
membership changes (the peer list is fixed at startup); and reads are only
ever served from whichever node the client happens to be connected to,
which is a deliberate, named simplification (eventual consistency for
`GET`/`KEYS`, not linearizable) rather than an oversight — a real
read-from-Leader or read-index scheme would be the next step if that
mattered here.

## Conventions

- Package layout: `store/` holds the storage engine (`store.go`) and WAL
  persistence (`wal.go`); `server/` holds the TCP protocol server
  (`server.go`); `raft/` holds Raft consensus — `state.go` persistent
  term/vote, `log.go` the in-memory entry log, `election.go`/`candidate.go`
  election decision logic, `append.go` replication decision logic,
  `transport.go` the TCP transport for both RPC kinds, `node.go` the
  `Node` type tying it all together (including the Leader-side replication
  loop); `cmd/kvstore` is the server entrypoint, `cmd/kvcli` the network
  client.
- Testing: table-free, one test function per behavior, in `_test.go` files
  next to the code under test (`store_test.go`, `wal_test.go`,
  `server_test.go`, and the `raft` package's `*_test.go` files, including
  `cluster_test.go`'s real multi-node integration test). Concurrency is
  tested by racing goroutines through the public API (see
  `TestConcurrentAccess`, `TestConcurrentClients`) — run with `go test -race`
  when touching locking (requires cgo; not available in every environment,
  e.g. this Windows dev box without a C toolchain).
- Raft was built with networking deliberately factored out: functions like
  `HandleRequestVote`/`HandleAppendEntries`/`StartElection`/`CountVotes`
  are pure (no I/O), and `Node` takes `RequestVoteSender`/
  `AppendEntriesSender`/`Applier` functions instead of concrete
  dependencies (a network client, a `*store.Store`), so consensus logic is
  fully unit-testable with fakes — `DialRequestVote`/`DialAppendEntries`
  (real TCP) and a real `Applier` are only wired in where a `Node` is
  actually run (`cmd/kvstore/main.go` is that wiring point).
- `server.New` (direct-to-store, stage 2 behavior) and `server.NewRaft`
  (routes `SET`/`DELETE` through a `*raft.Node`) share the same `Server`
  type and protocol — which constructor `cmd/kvstore` calls is decided
  purely by whether `--raft-addr` was passed.
- WAL format: newline-terminated text headers (`SET <keylen> <vallen>\n` /
  `DEL <keylen>\n`) followed by raw key/value bytes, sized by length prefix
  rather than delimiter so values can contain arbitrary bytes including
  newlines.
- Durability: every Set/Delete syncs to disk before mutating in-memory state,
  so a failed write leaves the store unchanged rather than partially applied.
- Network protocol is line-based text (see `server/server.go` doc comment),
  so unlike the WAL, keys/values sent over the wire can't contain newlines
  and keys can't contain spaces — a limitation inherited from the original
  REPL, not present in the storage engine itself.
