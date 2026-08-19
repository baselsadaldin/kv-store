package raft

import (
	"sync"
	"testing"
)

func TestDialRequestVoteRoundTrip(t *testing.T) {
	handle := func(args RequestVoteArgs) RequestVoteReply {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}
	}

	transport, err := ListenAndServe("127.0.0.1:0", handle)
	if err != nil {
		t.Fatalf("ListenAndServe failed: %v", err)
	}
	defer transport.Close()

	reply, err := DialRequestVote(transport.listener.Addr().String(), RequestVoteArgs{
		Term: 7, CandidateID: "node2",
	})
	if err != nil {
		t.Fatalf("DialRequestVote failed: %v", err)
	}
	if reply.Term != 7 || !reply.VoteGranted {
		t.Fatalf("got %+v, want {Term: 7, VoteGranted: true}", reply)
	}
}

func TestDialRequestVoteToUnreachablePeerReturnsError(t *testing.T) {
	// Nothing is listening on this port, so the dial (or the immediate
	// connection refusal) should surface as an error rather than hanging.
	_, err := DialRequestVote("127.0.0.1:1", RequestVoteArgs{Term: 1})
	if err == nil {
		t.Fatal("expected an error dialing an unreachable peer, got nil")
	}
}

func TestListenAndServeHandlesConcurrentRequests(t *testing.T) {
	handle := func(args RequestVoteArgs) RequestVoteReply {
		return RequestVoteReply{Term: args.Term, VoteGranted: args.CandidateID == "winner"}
	}

	transport, err := ListenAndServe("127.0.0.1:0", handle)
	if err != nil {
		t.Fatalf("ListenAndServe failed: %v", err)
	}
	defer transport.Close()
	addr := transport.listener.Addr().String()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, err := DialRequestVote(addr, RequestVoteArgs{Term: 1, CandidateID: "winner"})
			if err != nil {
				t.Errorf("DialRequestVote failed: %v", err)
				return
			}
			if !reply.VoteGranted {
				t.Errorf("got VoteGranted=false, want true")
			}
		}()
	}
	wg.Wait()
}

func TestCloseStopsAcceptingConnections(t *testing.T) {
	handle := func(args RequestVoteArgs) RequestVoteReply {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}
	}

	transport, err := ListenAndServe("127.0.0.1:0", handle)
	if err != nil {
		t.Fatalf("ListenAndServe failed: %v", err)
	}
	addr := transport.listener.Addr().String()

	if err := transport.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, err := DialRequestVote(addr, RequestVoteArgs{Term: 1}); err == nil {
		t.Fatal("expected DialRequestVote to fail after Close, got nil error")
	}
}
