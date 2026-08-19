package raft

import (
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// reserveAddrs grabs n free loopback ports by briefly binding and releasing
// them, so cluster nodes can be told each other's addresses before any of
// them start listening for real.
func reserveAddrs(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to reserve a port: %v", err)
		}
		addrs[i] = l.Addr().String()
		l.Close()
	}
	return addrs
}

// clusterNode bundles a real Node with its real Transport and its own
// local key-value map, standing in for store.Store without the raft
// package depending on it directly (see Applier).
type clusterNode struct {
	node      *Node
	transport *Transport
	mu        sync.Mutex
	data      map[string]string
}

func (c *clusterNode) get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.data[key]
	return v, ok
}

// TestThreeNodeClusterElectsLeaderReplicatesAndApplies is the end-to-end
// proof that every piece built across this package actually works
// together: three real Nodes, talking over real TCP transports on
// loopback, elect a single Leader on their own via the background timer
// loop, and a command Proposed on that Leader gets replicated, committed
// by majority, and applied to every node's own state machine -- without
// any test code driving the protocol by hand.
func TestThreeNodeClusterElectsLeaderReplicatesAndApplies(t *testing.T) {
	addrs := reserveAddrs(t, 3)

	nodes := make([]*clusterNode, len(addrs))
	for i, addr := range addrs {
		var peers []string
		for j, other := range addrs {
			if j != i {
				peers = append(peers, other)
			}
		}

		statePath := filepath.Join(t.TempDir(), "state")
		n, err := Open(addr, peers, statePath)
		if err != nil {
			t.Fatalf("Open failed for %s: %v", addr, err)
		}

		c := &clusterNode{node: n, data: make(map[string]string)}
		n.SetApplier(func(cmd Command) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			switch cmd.Op {
			case "SET":
				c.data[cmd.Key] = cmd.Value
			case "DEL":
				delete(c.data, cmd.Key)
			}
			return nil
		})

		transport, err := ListenAndServe(addr, Handlers{
			RequestVote:   n.HandleRequestVoteRPC,
			AppendEntries: n.HandleAppendEntriesRPC,
		})
		if err != nil {
			t.Fatalf("ListenAndServe failed for %s: %v", addr, err)
		}
		c.transport = transport
		nodes[i] = c
	}

	for _, c := range nodes {
		go c.node.Run(DialRequestVote, DialAppendEntries, randomElectionTimeout)
	}
	t.Cleanup(func() {
		for _, c := range nodes {
			c.node.Stop()
			c.transport.Close()
		}
	})

	leader := waitForSingleLeader(t, nodes, 3*time.Second)

	index, isLeader := leader.node.Propose(Command{Op: "SET", Key: "foo", Value: "bar"})
	if !isLeader {
		t.Fatal("Propose reported the chosen node is not Leader")
	}
	if index < 1 {
		t.Fatalf("Propose returned index %d, want >= 1", index)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		allApplied := true
		for _, c := range nodes {
			if v, ok := c.get("foo"); !ok || v != "bar" {
				allApplied = false
			}
		}
		if allApplied {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all nodes applied the committed entry within the deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForSingleLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *clusterNode
		count := 0
		for _, c := range nodes {
			if c.node.Role() == Leader {
				count++
				leader = c
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no single Leader emerged within the deadline")
	return nil
}
