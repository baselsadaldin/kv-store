// Package raft will implement Raft consensus across kvstore nodes. This is
// the first piece: durable storage for the two values a node must never
// forget across a crash, even before any networking or log replication
// exists.
package raft

import (
	"fmt"
	"os"
	"path/filepath"
)

// PersistentState holds the Raft state that must survive a crash: the
// highest term this node has seen, and who (if anyone) it voted for in that
// term. Both must be durable and saved before the node acts on them (e.g.
// before granting a vote or starting an election), since losing them could
// let a node vote twice in the same term after a restart.
type PersistentState struct {
	CurrentTerm int
	VotedFor    string // empty means no vote cast yet this term
}

// noVote is the on-disk placeholder for an empty VotedFor, since the file
// format is whitespace-delimited and can't represent an empty field.
const noVote = "-"

// LoadState reads the persistent state from path. If path doesn't exist yet
// (a node's first boot), it returns the zero value (term 0, no vote) rather
// than an error.
func LoadState(path string) (PersistentState, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, err
	}
	defer f.Close()

	var s PersistentState
	var votedFor string
	if _, err := fmt.Fscanf(f, "%d %s\n", &s.CurrentTerm, &votedFor); err != nil {
		return PersistentState{}, err
	}
	if votedFor != noVote {
		s.VotedFor = votedFor
	}
	return s, nil
}

// SaveState durably writes s to path, replacing whatever was there before.
// It writes to a temp file, syncs it, then atomically renames it into
// place, so a crash mid-write can never leave path holding a torn or
// half-written record.
func SaveState(path string, s PersistentState) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".raft-state-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	votedFor := s.VotedFor
	if votedFor == "" {
		votedFor = noVote
	}
	if _, err := fmt.Fprintf(tmp, "%d %s\n", s.CurrentTerm, votedFor); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}
