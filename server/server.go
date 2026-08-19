// Package server exposes a store.Store over a line-based TCP protocol so
// multiple clients can connect concurrently instead of embedding the store
// in a single local process.
//
// Protocol: each request is one newline-terminated line using the same
// command syntax as the original REPL (SET key value / GET key / DELETE key
// / KEYS / COMPACT). Responses are also newline-terminated:
//
//	SET/DELETE/COMPACT -> "OK" or "ERR <message>"
//	GET                -> "VALUE <value>", "NIL" (key not found), or "ERR <message>"
//	KEYS               -> zero or more "KEY <key>" lines, followed by "END"
//
// As with the original REPL, keys and values are line-based text: they may
// not contain newlines, and keys may not contain spaces.
//
// A Server created with NewRaft routes SET/DELETE through Raft consensus
// (see raft.Node.Propose) instead of writing to store directly: a write
// only returns OK once it's been replicated to a majority and applied, and
// a non-Leader node refuses writes with an ERR rather than accepting one it
// can't make durable. GET/KEYS/COMPACT are unchanged either way — they
// always read/act on this node's own local store, so a Follower that
// hasn't caught up yet can return stale or missing data for those; only
// SET/DELETE carry Raft's consistency guarantee.
package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/baselsadaldin/kv-store/raft"
	"github.com/baselsadaldin/kv-store/store"
)

// proposeTimeout bounds how long a Raft-backed SET/DELETE waits for its
// entry to be committed and applied before giving up and reporting an
// error to the client.
const proposeTimeout = 2 * time.Second

// Server accepts TCP connections and dispatches the line protocol against a
// shared store.Store. The store's own locking makes it safe to use from the
// many per-connection goroutines Serve spawns.
type Server struct {
	store    *store.Store
	raftNode *raft.Node // nil means no Raft: SET/DELETE write to store directly (see New vs NewRaft)
	listener net.Listener
}

// New creates a Server backed by s that writes directly to the store, with
// no Raft consensus involved. Call Serve to start accepting connections.
func New(s *store.Store) *Server {
	return &Server{store: s}
}

// NewRaft creates a Server backed by s whose SET/DELETE go through node
// (see raft.Node.Propose) instead of writing to s directly. The caller is
// responsible for having already wired node's Applier to apply committed
// commands to s (typically via s.Set/s.Delete) and for starting node's
// background loop (Node.Run/RunWithRealTiming) — NewRaft only wires the
// client-facing protocol to an already-configured node.
func NewRaft(s *store.Store, node *raft.Node) *Server {
	return &Server{store: s, raftNode: node}
}

// Serve accepts connections on l, handling each on its own goroutine, until
// Close is called or Accept returns an error. It always returns a non-nil
// error; a return caused by Close wraps net.ErrClosed.
func (srv *Server) Serve(l net.Listener) error {
	srv.listener = l
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go srv.handleConn(conn)
	}
}

// Close stops Serve from accepting new connections. It does not interrupt
// connections already being handled.
func (srv *Server) Close() error {
	if srv.listener == nil {
		return nil
	}
	return srv.listener.Close()
}

func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	w := bufio.NewWriter(conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		srv.dispatch(w, line)
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func (srv *Server) dispatch(w *bufio.Writer, line string) {
	fields := strings.SplitN(line, " ", 3)
	cmd := strings.ToUpper(fields[0])

	switch cmd {
	case "SET":
		if len(fields) != 3 {
			fmt.Fprintf(w, "ERR usage: SET key value\n")
			return
		}
		if srv.raftNode != nil {
			if err := srv.propose(raft.Command{Op: "SET", Key: fields[1], Value: fields[2]}); err != nil {
				fmt.Fprintf(w, "ERR %v\n", err)
				return
			}
			fmt.Fprintf(w, "OK\n")
			return
		}
		if err := srv.store.Set(fields[1], fields[2]); err != nil {
			fmt.Fprintf(w, "ERR %v\n", err)
			return
		}
		fmt.Fprintf(w, "OK\n")

	case "GET":
		if len(fields) != 2 {
			fmt.Fprintf(w, "ERR usage: GET key\n")
			return
		}
		v, err := srv.store.Get(fields[1])
		if errors.Is(err, store.ErrKeyNotFound) {
			fmt.Fprintf(w, "NIL\n")
			return
		}
		fmt.Fprintf(w, "VALUE %s\n", v)

	case "DELETE":
		if len(fields) != 2 {
			fmt.Fprintf(w, "ERR usage: DELETE key\n")
			return
		}
		if srv.raftNode != nil {
			if err := srv.propose(raft.Command{Op: "DEL", Key: fields[1]}); err != nil {
				fmt.Fprintf(w, "ERR %v\n", err)
				return
			}
			fmt.Fprintf(w, "OK\n")
			return
		}
		if err := srv.store.Delete(fields[1]); err != nil {
			fmt.Fprintf(w, "ERR %v\n", err)
			return
		}
		fmt.Fprintf(w, "OK\n")

	case "KEYS":
		for _, k := range srv.store.Keys() {
			fmt.Fprintf(w, "KEY %s\n", k)
		}
		fmt.Fprintf(w, "END\n")

	case "COMPACT":
		if err := srv.store.Compact(); err != nil {
			fmt.Fprintf(w, "ERR %v\n", err)
			return
		}
		fmt.Fprintf(w, "OK\n")

	default:
		fmt.Fprintf(w, "ERR unknown command: %s\n", fields[0])
	}
}

// propose submits cmd to Raft and blocks until it's committed and applied,
// or until proposeTimeout elapses. It returns an error either way a client
// should treat as "this write did not happen" — including when this node
// isn't currently the Leader, since only the Leader can accept writes.
func (srv *Server) propose(cmd raft.Command) error {
	index, isLeader := srv.raftNode.Propose(cmd)
	if !isLeader {
		return errors.New("not the leader")
	}
	return srv.raftNode.WaitApplied(index, proposeTimeout)
}
