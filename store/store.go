// Package store implements a simple thread-safe in-memory key-value store.
package store

import (
	"errors"
	"os"
	"sync"
)

// ErrKeyNotFound is returned when a requested key does not exist in the store.
var ErrKeyNotFound = errors.New("key not found")

// ErrNoLog is returned by operations that require a backing write-ahead log
// (such as Compact) when called on a store created with New.
var ErrNoLog = errors.New("store has no backing log")

// Store is a thread-safe in-memory key-value store, optionally backed by a
// write-ahead log on disk (see Open).
type Store struct {
	// using a READ/WRITE mutex to allow concurrent reads and exclusive writes
	mu                 sync.RWMutex
	data               map[string]string
	log                *os.File // nil for stores created with New (in-memory only)
	writesSinceCompact int      // records appended to log since the last compaction
}

// New creates an empty Store ready for use.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Set stores value under key, overwriting any existing value. For a store
// opened with Open, the write is appended to the log and synced to disk
// before being applied in memory; if that fails, the in-memory state is left
// unchanged and the error is returned. Once the write is durable, Set may
// also trigger an automatic log compaction (see maybeCompactLocked); an error
// from that step is also returned even though the Set itself already
// succeeded, since it signals the same underlying log is having I/O trouble.
func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil {
		if err := appendRecord(s.log, encodeSet(key, value)); err != nil {
			return err
		}
		s.writesSinceCompact++
	}
	s.data[key] = value
	if s.log != nil {
		if err := s.maybeCompactLocked(); err != nil {
			return err
		}
	}
	return nil
}

// Get returns the value stored under key, or ErrKeyNotFound if it doesn't exist.
func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	// We haven't found the key in the map, so we return an error indicating that the key was not found.
	if !ok {
		return "", ErrKeyNotFound
	}
	return v, nil
}

// Delete removes key from the store. It is a no-op if the key doesn't exist.
// For a store opened with Open, the deletion is appended to the log and
// synced to disk before being applied in memory. As with Set, this may
// trigger an automatic log compaction.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil {
		if err := appendRecord(s.log, encodeDelete(key)); err != nil {
			return err
		}
		s.writesSinceCompact++
	}
	delete(s.data, key)
	if s.log != nil {
		if err := s.maybeCompactLocked(); err != nil {
			return err
		}
	}
	return nil
}

// Has reports whether key exists in the store.
func (s *Store) Has(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[key]
	return ok
}

// Len returns the number of keys currently stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Keys returns a snapshot slice of all keys currently in the store.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}
