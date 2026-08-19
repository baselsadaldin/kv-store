package raft

import (
	"path/filepath"
	"testing"
)

func TestSaveAndLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")

	want := PersistentState{CurrentTerm: 5, VotedFor: "node2"}
	if err := SaveState(path, want); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	got, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestLoadMissingFileReturnsZeroValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")

	got, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	want := PersistentState{CurrentTerm: 0, VotedFor: ""}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSaveOverwritesPreviousState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")

	if err := SaveState(path, PersistentState{CurrentTerm: 1, VotedFor: "node1"}); err != nil {
		t.Fatalf("first SaveState failed: %v", err)
	}
	want := PersistentState{CurrentTerm: 2, VotedFor: "node3"}
	if err := SaveState(path, want); err != nil {
		t.Fatalf("second SaveState failed: %v", err)
	}

	got, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSaveAndLoadEmptyVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")

	want := PersistentState{CurrentTerm: 3, VotedFor: ""}
	if err := SaveState(path, want); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	got, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
