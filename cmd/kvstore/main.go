// Command kvstore runs the key-value store as a TCP server. Use cmd/kvcli to
// connect to it.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/baselsadaldin/kv-store/server"
	"github.com/baselsadaldin/kv-store/store"
)

func main() {
	file := flag.String("file", "", "path to a write-ahead log file for persistence (in-memory only if omitted)")
	addr := flag.String("addr", ":6380", "address to listen on")
	flag.Parse()

	var s *store.Store
	if *file != "" {
		var err error
		s, err = store.Open(*file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", *file, err)
			os.Exit(1)
		}
	} else {
		s = store.New()
	}
	defer s.Close()

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen on %s: %v\n", *addr, err)
		os.Exit(1)
	}

	srv := server.New(s)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		srv.Close()
	}()

	fmt.Printf("kvstore server listening on %s\n", *addr)
	if err := srv.Serve(l); err != nil && !errors.Is(err, net.ErrClosed) {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
