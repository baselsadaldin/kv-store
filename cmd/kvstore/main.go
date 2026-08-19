// Command kvstore runs the key-value store as a TCP server. Use cmd/kvcli to
// connect to it.
//
// By default it runs as a standalone, single-node server (stage 2
// behavior). Passing --raft-addr turns it into one node of a Raft cluster:
// SET/DELETE are then only accepted once replicated to a majority of the
// nodes listed in --raft-peers (see server.NewRaft).
package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/baselsadaldin/kv-store/raft"
	"github.com/baselsadaldin/kv-store/server"
	"github.com/baselsadaldin/kv-store/store"
)

func main() {
	file := flag.String("file", "", "path to a write-ahead log file for persistence (in-memory only if omitted)")
	addr := flag.String("addr", ":6380", "address for client connections")
	raftAddr := flag.String("raft-addr", "", "address for this node's Raft peer-to-peer RPCs; enables Raft mode if set")
	raftPeers := flag.String("raft-peers", "", "comma-separated --raft-addr values of the other nodes in the cluster (Raft mode only)")
	raftState := flag.String("raft-state", "", "path to persist this node's Raft term/vote (required in Raft mode)")
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

	var srv *server.Server
	var raftNode *raft.Node
	var raftTransport *raft.Transport

	if *raftAddr != "" {
		if *raftState == "" {
			fmt.Fprintln(os.Stderr, "--raft-state is required when --raft-addr is set")
			os.Exit(1)
		}

		var peers []string
		for _, p := range strings.Split(*raftPeers, ",") {
			if p = strings.TrimSpace(p); p != "" {
				peers = append(peers, p)
			}
		}

		raftNode, err = raft.Open(*raftAddr, peers, *raftState)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open Raft state %s: %v\n", *raftState, err)
			os.Exit(1)
		}
		raftNode.SetApplier(func(cmd raft.Command) error {
			switch cmd.Op {
			case "SET":
				return s.Set(cmd.Key, cmd.Value)
			case "DEL":
				return s.Delete(cmd.Key)
			}
			return nil
		})

		raftTransport, err = raft.ListenAndServe(*raftAddr, raft.Handlers{
			RequestVote:   raftNode.HandleRequestVoteRPC,
			AppendEntries: raftNode.HandleAppendEntriesRPC,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to listen for Raft RPCs on %s: %v\n", *raftAddr, err)
			os.Exit(1)
		}

		go raftNode.RunWithRealTiming(raft.DialRequestVote, raft.DialAppendEntries)

		srv = server.NewRaft(s, raftNode)
		fmt.Printf("raft: node %s, peers %v, state %s\n", *raftAddr, peers, *raftState)
	} else {
		srv = server.New(s)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		srv.Close()
		if raftNode != nil {
			raftNode.Stop()
			raftTransport.Close()
		}
	}()

	fmt.Printf("kvstore server listening on %s\n", *addr)
	if err := srv.Serve(l); err != nil && !errors.Is(err, net.ErrClosed) {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
