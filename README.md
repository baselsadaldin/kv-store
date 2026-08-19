# kv-store

A distributed key-value store written in Go, built as a portfolio project to
demonstrate systems and distributed systems fundamentals: a durable storage
engine, a TCP client-server protocol, and Raft-based consensus replicating
it across multiple nodes with automatic leader election and failover.

## Packages

- [`store`](./store) — the storage engine: `Set`, `Get`, `Delete`, `Has`, `Len`, `Keys`,
  backed by an optional write-ahead log (`Open`, `Close`, `Compact`) for
  crash-safe persistence.
- [`raft`](./raft) — a Raft consensus implementation: leader election,
  log replication, and crash-safe persistence, independent of any
  particular state machine (see `Node`, `Applier`).
- [`server`](./server) — a TCP server that exposes a `store.Store` over a
  line-based text protocol, either standalone (`New`) or with `SET`/`DELETE`
  routed through Raft consensus (`NewRaft`).
- [`cmd/kvstore`](./cmd/kvstore) — the server binary; add `--raft-addr` to
  run it as one node of a cluster.
- [`cmd/kvcli`](./cmd/kvcli) — an interactive REPL client that connects to a
  running `kvstore` server over TCP.

## Features

- Thread-safe in-memory store (`sync.RWMutex`, concurrent reads / exclusive writes)
- Optional durability via a write-ahead log: every `Set`/`Delete` is synced to
  disk before it's applied in memory, so a failed write can't leave the store
  partially updated
- Crash recovery: a torn write left by a crash mid-append is detected and
  discarded on reopen, truncating the log back to its last valid record
- Log compaction, manual (`Compact`) or automatic (once the log holds enough
  dead records relative to live keys), rewritten via a temp file + atomic
  rename so a crash mid-compaction can't corrupt the log
- Served over TCP so multiple clients can connect and operate on the same
  store concurrently
- Raft consensus: automatic leader election with randomized timeouts, log
  replication with conflict resolution, majority-based commit, and durable
  persistence so a crashed node can never forget a vote it already cast —
  writes are only acknowledged once replicated to a majority of the cluster
  and applied, and a cluster keeps accepting writes after losing its Leader
  once the survivors elect a new one

## Usage as a library

```go
s, err := store.Open("kv.log") // omit for an in-memory-only store: store.New()
if err != nil {
    // ...
}
defer s.Close()

s.Set("foo", "bar")

v, err := s.Get("foo")
if err != nil {
    // errors.Is(err, store.ErrKeyNotFound)
}
```

## Running the server and CLI

Start the server, optionally pointing it at a log file for persistence:

```sh
go run ./cmd/kvstore --addr :6380 --file kv.log
```

Connect with the client in another terminal:

```sh
go run ./cmd/kvcli --addr 127.0.0.1:6380
```

```
kvcli> connected to 127.0.0.1:6380 - commands: SET key value | GET key | DELETE key | KEYS | COMPACT | EXIT
> SET foo bar
OK
> GET foo
bar
> KEYS
foo
END
> DELETE foo
OK
> GET foo
(nil)
```

Keys and values are line-based text: they can't contain newlines, and keys
can't contain spaces.

## Running a Raft cluster

Each node needs a client address (`--addr`), a Raft peer-to-peer address
(`--raft-addr`, which also serves as that node's identity), the other
nodes' `--raft-addr` values (`--raft-peers`), and a file to persist its
Raft term/vote (`--raft-state`):

```sh
go run ./cmd/kvstore --addr 127.0.0.1:6301 --raft-addr 127.0.0.1:7301 \
  --raft-peers 127.0.0.1:7302,127.0.0.1:7303 --raft-state raft1.state

go run ./cmd/kvstore --addr 127.0.0.1:6302 --raft-addr 127.0.0.1:7302 \
  --raft-peers 127.0.0.1:7301,127.0.0.1:7303 --raft-state raft2.state

go run ./cmd/kvstore --addr 127.0.0.1:6303 --raft-addr 127.0.0.1:7303 \
  --raft-peers 127.0.0.1:7301,127.0.0.1:7302 --raft-state raft3.state
```

The three nodes elect a Leader among themselves within a few hundred
milliseconds. Only the Leader accepts `SET`/`DELETE` — any other node
returns `ERR not the leader`, so point `kvcli` at whichever node currently
wins (or just retry against a different one). `GET`/`KEYS`/`COMPACT` always
read/act on whichever node you're connected to, so a Follower that hasn't
caught up yet can return stale data for those. If the Leader's process
dies, the remaining nodes elect a new one and the cluster keeps accepting
writes, with no data lost from before the crash.

## Testing

```sh
go test ./...
```

Concurrency-sensitive code is tested by racing goroutines through the public
API; run with `go test -race ./...` when touching locking (requires cgo).
