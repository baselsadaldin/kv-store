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

Stages 1-2 complete, stage 3 (Raft) in progress.

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

Stage 3 (in progress): Raft consensus, being built incrementally in a new
`raft` package, deliberately kept independent of networking so each piece
is unit-testable in isolation before any TCP is involved. So far:
`PersistentState` (durable `currentTerm`/`votedFor`, same write-temp-file +
atomic-rename crash safety as `store`'s log compaction); an in-memory `Log`
of term-tagged entries (`Append`/`Get`/`LastIndex`/`LastTerm`/
`TruncateFrom`); `HandleRequestVote` (the receiving side of an election —
term/vote/log-up-to-date checks); `StartElection` and `CountVotes` (the
candidate side — becoming a candidate and tallying replies); and `Node`,
which ties those together into `RunElection` (fans a `RequestVote` out to
peers via an injected `RequestVoteSender` function and transitions
role/state on the result). Not yet built: real peer-to-peer transport (a
separate port from the client protocol, using `encoding/gob` — decided but
not implemented), the background timer loop that actually triggers
elections during normal operation, `AppendEntries`/log replication, and
wiring committed entries through to `store.Set`/`store.Delete`.

Next up: continuing stage 3 — transport, then `AppendEntries`.

## Conventions

- Package layout: `store/` holds the storage engine (`store.go`) and WAL
  persistence (`wal.go`); `server/` holds the TCP protocol server
  (`server.go`); `raft/` holds Raft consensus (`state.go` persistent
  term/vote, `log.go` the in-memory entry log, `election.go`/`candidate.go`
  the vote request/grant/tally logic, `node.go` the `Node` type tying it
  together); `cmd/kvstore` is the server entrypoint, `cmd/kvcli` the network
  client.
- Testing: table-free, one test function per behavior, in `_test.go` files
  next to the code under test (`store_test.go`, `wal_test.go`,
  `server_test.go`, and the `raft` package's `*_test.go` files). Concurrency
  is tested by racing goroutines through the public API (see
  `TestConcurrentAccess`, `TestConcurrentClients`) — run with `go test -race`
  when touching locking (requires cgo; not available in every environment,
  e.g. this Windows dev box without a C toolchain).
- Raft is being built with networking deliberately factored out: functions
  like `HandleRequestVote`/`StartElection`/`CountVotes` are pure (no I/O),
  and `Node.RunElection` takes a `RequestVoteSender` function instead of a
  concrete network client, so election logic is fully unit-testable with a
  fake transport before any real TCP code exists.
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
