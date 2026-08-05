package store

import (
	"errors"
	"sync"
	"testing"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("foo", "bar")

	v, err := s.Get("foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "bar" {
		t.Fatalf("got %q, want %q", v, "bar")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	_, err := s.Get("missing")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("got %v, want ErrKeyNotFound", err)
	}
}

func TestOverwrite(t *testing.T) {
	s := New()
	s.Set("foo", "bar")
	s.Set("foo", "baz")

	v, err := s.Get("foo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "baz" {
		t.Fatalf("got %q, want %q", v, "baz")
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Set("foo", "bar")
	s.Delete("foo")

	if s.Has("foo") {
		t.Fatal("expected key to be deleted")
	}
	if _, err := s.Get("foo"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("got %v, want ErrKeyNotFound", err)
	}
}

func TestDeleteMissing(t *testing.T) {
	s := New()
	s.Delete("missing") // should not panic
}

func TestLenAndKeys(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")

	if s.Len() != 2 {
		t.Fatalf("got Len() = %d, want 2", s.Len())
	}

	keys := s.Keys()
	if len(keys) != 2 {
		t.Fatalf("got %d keys, want 2", len(keys))
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Set("key", "value")
			s.Get("key")
			s.Has("key")
		}(i)
	}
	wg.Wait()
}
