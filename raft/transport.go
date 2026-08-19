// Peer-to-peer Raft RPCs travel over their own TCP listener, separate from
// the client-facing line protocol in package server, since the two have
// different shapes (structured, nested data here vs. flat text commands)
// and different trust boundaries (only other cluster nodes should ever
// speak this protocol). Messages are Go values encoded with encoding/gob:
// one request, one reply, per connection.
package raft

import (
	"encoding/gob"
	"net"
	"time"
)

// dialTimeout bounds how long DialRequestVote waits to connect to a peer,
// so an unreachable node can't hang a caller indefinitely.
const dialTimeout = 200 * time.Millisecond

// RequestVoteHandler processes an incoming RequestVoteArgs and returns the
// reply to send back. It must durably persist any state changes (via
// SaveState) before returning, so the reply an implementation returns can
// be trusted to have already been made safe against a crash (see
// PersistentState and HandleRequestVote).
type RequestVoteHandler func(RequestVoteArgs) RequestVoteReply

// Transport listens for incoming Raft RPCs on one TCP address.
type Transport struct {
	listener net.Listener
}

// ListenAndServe starts listening on addr and handles incoming
// RequestVote RPCs with handle, one connection per request, until Close is
// called.
func ListenAndServe(addr string, handle RequestVoteHandler) (*Transport, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &Transport{listener: l}
	go t.serve(handle)
	return t, nil
}

func (t *Transport) serve(handle RequestVoteHandler) {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			return
		}
		go t.handleConn(conn, handle)
	}
}

func (t *Transport) handleConn(conn net.Conn, handle RequestVoteHandler) {
	defer conn.Close()

	var args RequestVoteArgs
	if err := gob.NewDecoder(conn).Decode(&args); err != nil {
		return
	}

	reply := handle(args)
	gob.NewEncoder(conn).Encode(reply)
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

	if err := gob.NewEncoder(conn).Encode(args); err != nil {
		return RequestVoteReply{}, err
	}

	var reply RequestVoteReply
	if err := gob.NewDecoder(conn).Decode(&reply); err != nil {
		return RequestVoteReply{}, err
	}
	return reply, nil
}
