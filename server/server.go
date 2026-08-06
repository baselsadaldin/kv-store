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
package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/baselsadaldin/kv-store/store"
)

// Server accepts TCP connections and dispatches the line protocol against a
// shared store.Store. The store's own locking makes it safe to use from the
// many per-connection goroutines Serve spawns.
type Server struct {
	store    *store.Store
	listener net.Listener
}

// New creates a Server backed by s. Call Serve to start accepting connections.
func New(s *store.Store) *Server {
	return &Server{store: s}
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
