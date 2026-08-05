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

Stage 1 (single-node) complete. In-memory store with a write-ahead log for
persistence: durable Set/Delete (fsync on every write), crash recovery from
torn writes on reopen, and log compaction (manual via `Compact`, or automatic
once the log holds ≥100 records and ≥4x the live key count) to bound log
growth. CLI REPL (`cmd/kvstore`) exposes SET/GET/DELETE/KEYS/COMPACT,
optionally backed by a log file via `--file`.

Next up: Stage 2, networking & client-server layer.

## Conventions

- Package layout: `store/` holds the storage engine (`store.go`) and WAL
  persistence (`wal.go`), kept as separate files but one package; `cmd/kvstore`
  is the CLI entrypoint.
- Testing: table-free, one test function per behavior, in `_test.go` files
  next to the code under test (`store_test.go`, `wal_test.go`). Concurrency is
  tested by racing goroutines through the public API (see
  `TestConcurrentAccess`) — run with `go test -race` when touching locking.
- WAL format: newline-terminated text headers (`SET <keylen> <vallen>\n` /
  `DEL <keylen>\n`) followed by raw key/value bytes, sized by length prefix
  rather than delimiter so values can contain arbitrary bytes including
  newlines.
- Durability: every Set/Delete syncs to disk before mutating in-memory state,
  so a failed write leaves the store unchanged rather than partially applied.
