# kv-store

A key-value store written in Go, built as a portfolio project to demonstrate
systems and distributed systems fundamentals. It's being developed in four
stages: single-node storage, a networked client-server layer, Raft-based
replication, and production polish. Stages 1-2 are done.

## Packages

- [`store`](./store) — the storage engine: `Set`, `Get`, `Delete`, `Has`, `Len`, `Keys`,
  backed by an optional write-ahead log (`Open`, `Close`, `Compact`) for
  crash-safe persistence.
- [`server`](./server) — a TCP server that exposes a `store.Store` over a
  line-based text protocol, handling each connection concurrently.
- [`cmd/kvstore`](./cmd/kvstore) — the server binary.
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

## Testing

```sh
go test ./...
```

Concurrency-sensitive code is tested by racing goroutines through the public
API; run with `go test -race ./...` when touching locking (requires cgo).
