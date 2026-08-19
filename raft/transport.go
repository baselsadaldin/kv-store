// Peer-to-peer Raft RPCs travel over their own TCP listener, separate from
// the client-facing line protocol in package server, since the two have
// different shapes (structured, nested data here vs. flat text commands)
// and different trust boundaries (only other cluster nodes should ever
// speak this protocol). Both RPC kinds share one port: each connection
// starts with a single tag byte identifying which RPC it carries, followed
// by one gob-encoded request and one gob-encoded reply.
package raft

import (
	"encoding/gob"
	"io"
	"net"
	"time"
)

// dialTimeout bounds how long DialRequestVote/DialAppendEntries wait to
// connect to a peer, so an unreachable node can't hang a caller
// indefinitely.
const dialTimeout = 200 * time.Millisecond

// rpcKind tags which RPC a connection carries, so ListenAndServe can
// dispatch to the right handler over the one shared port.
type rpcKind byte

const (
	rpcRequestVote rpcKind = iota
	rpcAppendEntries
)

// RequestVoteHandler processes an incoming RequestVoteArgs and returns the
// reply to send back. It must durably persist any state changes (via
// SaveState) before returning, so the reply an implementation returns can
// be trusted to have already been made safe against a crash (see
// PersistentState and HandleRequestVote).
type RequestVoteHandler func(RequestVoteArgs) RequestVoteReply

// AppendEntriesHandler processes an incoming AppendEntriesArgs and returns
// the reply to send back, with the same durability contract as
// RequestVoteHandler.
type AppendEntriesHandler func(AppendEntriesArgs) AppendEntriesReply

// Handlers bundles the two RPC handlers ListenAndServe dispatches to.
type Handlers struct {
	RequestVote   RequestVoteHandler
	AppendEntries AppendEntriesHandler
}

// Transport listens for incoming Raft RPCs on one TCP address.
type Transport struct {
	listener net.Listener
}

// ListenAndServe starts listening on addr and dispatches incoming RPCs to
// the matching handler in handlers, one connection per request, until
// Close is called.
func ListenAndServe(addr string, handlers Handlers) (*Transport, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &Transport{listener: l}
	go t.serve(handlers)
	return t, nil
}

func (t *Transport) serve(handlers Handlers) {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			return
		}
		go t.handleConn(conn, handlers)
	}
}

func (t *Transport) handleConn(conn net.Conn, handlers Handlers) {
	defer conn.Close()

	var kind [1]byte
	if _, err := io.ReadFull(conn, kind[:]); err != nil {
		return
	}

	switch rpcKind(kind[0]) {
	case rpcRequestVote:
		var args RequestVoteArgs
		if err := gob.NewDecoder(conn).Decode(&args); err != nil {
			return
		}
		gob.NewEncoder(conn).Encode(handlers.RequestVote(args))

	case rpcAppendEntries:
		var args AppendEntriesArgs
		if err := gob.NewDecoder(conn).Decode(&args); err != nil {
			return
		}
		gob.NewEncoder(conn).Encode(handlers.AppendEntries(args))
	}
}

// Close stops the Transport from accepting new connections.
func (t *Transport) Close() error {
	return t.listener.Close()
}

// DialRequestVote sends a RequestVote RPC to peer and returns its reply. Its
// signature matches RequestVoteSender, so it can be passed directly to
// Node.RunElection as the real, networked implementation.
func DialRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	conn, err := net.DialTimeout("tcp", peer, dialTimeout)
	if err != nil {
		return RequestVoteReply{}, err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{byte(rpcRequestVote)}); err != nil {
		return RequestVoteReply{}, err
	}
	if err := gob.NewEncoder(conn).Encode(args); err != nil {
		return RequestVoteReply{}, err
	}

	var reply RequestVoteReply
	if err := gob.NewDecoder(conn).Decode(&reply); err != nil {
		return RequestVoteReply{}, err
	}
	return reply, nil
}

// DialAppendEntries sends an AppendEntries RPC to peer and returns its
// reply. Its signature matches AppendEntriesSender, so it can be passed
// directly to Node's replication loop as the real, networked
// implementation.
func DialAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	conn, err := net.DialTimeout("tcp", peer, dialTimeout)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{byte(rpcAppendEntries)}); err != nil {
		return AppendEntriesReply{}, err
	}
	if err := gob.NewEncoder(conn).Encode(args); err != nil {
		return AppendEntriesReply{}, err
	}

	var reply AppendEntriesReply
	if err := gob.NewDecoder(conn).Decode(&reply); err != nil {
		return AppendEntriesReply{}, err
	}
	return reply, nil
}
