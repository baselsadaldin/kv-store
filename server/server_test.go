package server

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/baselsadaldin/kv-store/store"
)

// startTestServer starts a Server on a loopback port chosen by the OS and
// returns a dialer for connecting to it. The server is stopped when the test
// completes.
func startTestServer(t *testing.T, s *store.Store) func() net.Conn {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	srv := New(s)
	done := make(chan struct{})
	go func() {
		srv.Serve(l)
		close(done)
	}()
	t.Cleanup(func() {
		srv.Close()
		<-done
	})

	return func() net.Conn {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
}

// sendCommand writes line to conn and returns the single response line, with
// the trailing newline stripped.
func sendCommand(t *testing.T, conn net.Conn, r *bufio.Reader, line string) string {
	t.Helper()
	if _, err := conn.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	resp, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	return strings.TrimRight(resp, "\n")
}

func TestSetGetOverNetwork(t *testing.T) {
	dial := startTestServer(t, store.New())
	conn := dial()
	r := bufio.NewReader(conn)

	if got := sendCommand(t, conn, r, "SET foo bar"); got != "OK" {
		t.Fatalf("SET: got %q, want OK", got)
	}
	if got := sendCommand(t, conn, r, "GET foo"); got != "VALUE bar" {
		t.Fatalf("GET: got %q, want VALUE bar", got)
	}
}

func TestGetMissingOverNetwork(t *testing.T) {
	dial := startTestServer(t, store.New())
	conn := dial()
	r := bufio.NewReader(conn)

	if got := sendCommand(t, conn, r, "GET missing"); got != "NIL" {
		t.Fatalf("got %q, want NIL", got)
	}
}

func TestDeleteOverNetwork(t *testing.T) {
	dial := startTestServer(t, store.New())
	conn := dial()
	r := bufio.NewReader(conn)

	sendCommand(t, conn, r, "SET foo bar")
	if got := sendCommand(t, conn, r, "DELETE foo"); got != "OK" {
		t.Fatalf("DELETE: got %q, want OK", got)
	}
	if got := sendCommand(t, conn, r, "GET foo"); got != "NIL" {
		t.Fatalf("GET after DELETE: got %q, want NIL", got)
	}
}

func TestKeysOverNetworkTerminatesWithEnd(t *testing.T) {
	dial := startTestServer(t, store.New())
	conn := dial()
	r := bufio.NewReader(conn)

	sendCommand(t, conn, r, "SET a 1")
	conn.Write([]byte("SET b 2\n"))
	r.ReadString('\n') // discard OK for SET b

	conn.Write([]byte("KEYS\n"))
	got := make(map[string]bool)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		line = strings.TrimRight(line, "\n")
		if line == "END" {
			break
		}
		got[strings.TrimPrefix(line, "KEY ")] = true
	}
	if !got["a"] || !got["b"] || len(got) != 2 {
		t.Fatalf("got keys %v, want exactly {a, b}", got)
	}
}

func TestCompactOverNetwork(t *testing.T) {
	path := t.TempDir() + "/kv.log"
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	dial := startTestServer(t, s)
	conn := dial()
	r := bufio.NewReader(conn)

	if got := sendCommand(t, conn, r, "COMPACT"); got != "OK" {
		t.Fatalf("COMPACT: got %q, want OK", got)
	}
}

func TestUnknownCommandReturnsErr(t *testing.T) {
	dial := startTestServer(t, store.New())
	conn := dial()
	r := bufio.NewReader(conn)

	got := sendCommand(t, conn, r, "FROBNICATE foo")
	if !strings.HasPrefix(got, "ERR ") {
		t.Fatalf("got %q, want an ERR response", got)
	}
}

func TestMalformedSetReturnsErr(t *testing.T) {
	dial := startTestServer(t, store.New())
	conn := dial()
	r := bufio.NewReader(conn)

	got := sendCommand(t, conn, r, "SET onlykey")
	if !strings.HasPrefix(got, "ERR ") {
		t.Fatalf("got %q, want an ERR response", got)
	}
}

func TestConcurrentClients(t *testing.T) {
	dial := startTestServer(t, store.New())

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := dial()
			r := bufio.NewReader(conn)
			sendCommand(t, conn, r, "SET key value")
			sendCommand(t, conn, r, "GET key")
		}()
	}
	wg.Wait()
}
