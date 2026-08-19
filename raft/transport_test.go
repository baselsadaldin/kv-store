package raft

import (
	"sync"
	"testing"
)

func testHandlers(voteHandle RequestVoteHandler, appendHandle AppendEntriesHandler) Handlers {
	if voteHandle == nil {
		voteHandle = func(RequestVoteArgs) RequestVoteReply { return RequestVoteReply{} }
	}
	if appendHandle == nil {
		appendHandle = func(AppendEntriesArgs) AppendEntriesReply { return AppendEntriesReply{} }
	}
	return Handlers{RequestVote: voteHandle, AppendEntries: appendHandle}
}

func TestDialRequestVoteRoundTrip(t *testing.T) {
	handlers := testHandlers(func(args RequestVoteArgs) RequestVoteReply {
		return RequestVoteReply{Term: args.Term, VoteGranted: true}
	}, nil)

	transport, err := ListenAndServe("127.0.0.1:0", handlers)
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

func TestDialAppendEntriesRoundTrip(t *testing.T) {
	handlers := testHandlers(nil, func(args AppendEntriesArgs) AppendEntriesReply {
		return AppendEntriesReply{Term: args.Term, Success: true}
	})

	transport, err := ListenAndServe("127.0.0.1:0", handlers)
	if err != nil {
		t.Fatalf("ListenAndServe failed: %v", err)
	}
	defer transport.Close()

	reply, err := DialAppendEntries(transport.listener.Addr().String(), AppendEntriesArgs{
		Term: 3, LeaderID: "node1",
	})
	if err != nil {
		t.Fatalf("DialAppendEntries failed: %v", err)
	}
	if reply.Term != 3 || !reply.Success {
		t.Fatalf("got %+v, want {Term: 3, Success: true}", reply)
	}
}

func TestDialAppendEntriesToUnreachablePeerReturnsError(t *testing.T) {
	_, err := DialAppendEntries("127.0.0.1:1", AppendEntriesArgs{Term: 1})
	if err == nil {
		t.Fatal("expected an error dialing an unreachable peer, got nil")
	}
}

func TestOnePortDispatchesBothRPCKindsCorrectly(t *testing.T) {
	handlers := testHandlers(
		func(args RequestVoteArgs) RequestVoteReply {
			return RequestVoteReply{Term: args.Term, VoteGranted: true}
		},
		func(args AppendEntriesArgs) AppendEntriesReply {
			return AppendEntriesReply{Term: args.Term, Success: true}
		},
	)

	transport, err := ListenAndServe("127.0.0.1:0", handlers)
	if err != nil {
		t.Fatalf("ListenAndServe failed: %v", err)
	}
	defer transport.Close()
	addr := transport.listener.Addr().String()

	voteReply, err := DialRequestVote(addr, RequestVoteArgs{Term: 1, CandidateID: "node2"})
	if err != nil {
		t.Fatalf("DialRequestVote failed: %v", err)
	}
	if !voteReply.VoteGranted {
		t.Fatal("expected the RequestVote handler to be the one that answered")
	}

	appendReply, err := DialAppendEntries(addr, AppendEntriesArgs{Term: 1, LeaderID: "node2"})
	if err != nil {
		t.Fatalf("DialAppendEntries failed: %v", err)
	}
	if !appendReply.Success {
		t.Fatal("expected the AppendEntries handler to be the one that answered")
	}
}

func TestListenAndServeHandlesConcurrentRequests(t *testing.T) {
	handlers := testHandlers(func(args RequestVoteArgs) RequestVoteReply {
		return RequestVoteReply{Term: args.Term, VoteGranted: args.CandidateID == "winner"}
	}, nil)

	transport, err := ListenAndServe("127.0.0.1:0", handlers)
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
	transport, err := ListenAndServe("127.0.0.1:0", testHandlers(nil, nil))
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
