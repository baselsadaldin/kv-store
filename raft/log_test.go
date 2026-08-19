package raft

import "testing"

func TestAppendAssignsSequentialIndices(t *testing.T) {
	l := NewLog()

	e1 := l.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	e2 := l.Append(1, Command{Op: "SET", Key: "b", Value: "2"})
	e3 := l.Append(2, Command{Op: "DEL", Key: "a"})

	if e1.Index != 1 || e2.Index != 2 || e3.Index != 3 {
		t.Fatalf("got indices %d, %d, %d, want 1, 2, 3", e1.Index, e2.Index, e3.Index)
	}
}

func TestGetReturnsEntryByIndex(t *testing.T) {
	l := NewLog()
	want := l.Append(3, Command{Op: "SET", Key: "foo", Value: "bar"})

	got, ok := l.Get(1)
	if !ok {
		t.Fatal("Get(1) returned ok=false, want true")
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestGetMissingIndexReturnsFalse(t *testing.T) {
	l := NewLog()
	l.Append(1, Command{Op: "SET", Key: "a", Value: "1"})

	if _, ok := l.Get(0); ok {
		t.Fatal("Get(0) returned ok=true, want false")
	}
	if _, ok := l.Get(5); ok {
		t.Fatal("Get(5) returned ok=true, want false")
	}
}

func TestLastIndexAndLastTermOnEmptyLog(t *testing.T) {
	l := NewLog()

	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0", got)
	}
	if got := l.LastTerm(); got != 0 {
		t.Fatalf("LastTerm() = %d, want 0", got)
	}
}

func TestLastIndexAndLastTermAfterAppends(t *testing.T) {
	l := NewLog()
	l.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	l.Append(4, Command{Op: "SET", Key: "b", Value: "2"})

	if got := l.LastIndex(); got != 2 {
		t.Fatalf("LastIndex() = %d, want 2", got)
	}
	if got := l.LastTerm(); got != 4 {
		t.Fatalf("LastTerm() = %d, want 4", got)
	}
}

func TestTruncateFromRemovesEntryAndLater(t *testing.T) {
	l := NewLog()
	l.Append(1, Command{Op: "SET", Key: "a", Value: "1"})
	l.Append(1, Command{Op: "SET", Key: "b", Value: "2"})
	l.Append(2, Command{Op: "SET", Key: "c", Value: "3"})

	l.TruncateFrom(2)

	if got := l.LastIndex(); got != 1 {
		t.Fatalf("LastIndex() = %d, want 1", got)
	}
	if _, ok := l.Get(2); ok {
		t.Fatal("Get(2) returned ok=true after truncation, want false")
	}
}

func TestTruncateFromOnEmptyLogIsNoOp(t *testing.T) {
	l := NewLog()
	l.TruncateFrom(1)

	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0", got)
	}
}
