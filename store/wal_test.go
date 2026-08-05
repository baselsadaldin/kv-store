package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	entries := map[string]string{
		"foo":           "bar",
		"with space":    "has space",
		"with\ttab":     "has\ttab",
		"with\nnewline": "has\nnewline",
	}
	for k, v := range entries {
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set(%q, %q) failed: %v", k, v, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	for k, want := range entries {
		got, err := reopened.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", k, err)
		}
		if got != want {
			t.Fatalf("Get(%q) = %q, want %q", k, got, want)
		}
	}
}

func TestDeletePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := s.Set("foo", "bar"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := s.Delete("foo"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	if reopened.Has("foo") {
		t.Fatal("expected deleted key to stay deleted after reopen")
	}
}

func TestTornWriteRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")

	valid := encodeSet("a", "1")
	torn := encodeSet("bbbbb", "ccccc")
	torn = torn[:len(torn)-3] // simulate a crash partway through writing this record

	raw := append(append([]byte{}, valid...), torn...)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("failed to write fixture: %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error on torn log: %v", err)
	}
	defer s.Close()

	v, err := s.Get("a")
	if err != nil || v != "1" {
		t.Fatalf("got (%q, %v), want (\"1\", nil)", v, err)
	}
	if s.Has("bbbbb") {
		t.Fatal("torn record should not have been applied")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if info.Size() != int64(len(valid)) {
		t.Fatalf("got file size %d, want %d (log should be truncated at last valid record)", info.Size(), len(valid))
	}

	if err := s.Set("new", "value"); err != nil {
		t.Fatalf("Set after recovering from torn write failed: %v", err)
	}
}

func TestCompactShrinksLogAndPreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	// Overwrite and delete so the log accumulates dead records that
	// compaction should drop.
	for i := 0; i < 10; i++ {
		if err := s.Set("foo", "bar"); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}
	if err := s.Set("keep", "me"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := s.Set("gone", "soon"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := s.Delete("gone"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	sizeBefore, err := logSize(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	sizeAfter, err := logSize(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if sizeAfter >= sizeBefore {
		t.Fatalf("expected log to shrink after Compact, got %d -> %d", sizeBefore, sizeAfter)
	}

	if v, err := s.Get("foo"); err != nil || v != "bar" {
		t.Fatalf("got (%q, %v), want (\"bar\", nil)", v, err)
	}
	if v, err := s.Get("keep"); err != nil || v != "me" {
		t.Fatalf("got (%q, %v), want (\"me\", nil)", v, err)
	}
	if s.Has("gone") {
		t.Fatal("deleted key resurfaced after Compact")
	}

	// State must also survive a reopen against the compacted log.
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after compact failed: %v", err)
	}
	defer reopened.Close()

	if v, err := reopened.Get("foo"); err != nil || v != "bar" {
		t.Fatalf("got (%q, %v), want (\"bar\", nil)", v, err)
	}
	if v, err := reopened.Get("keep"); err != nil || v != "me" {
		t.Fatalf("got (%q, %v), want (\"me\", nil)", v, err)
	}
	if reopened.Has("gone") {
		t.Fatal("deleted key resurfaced after reopen")
	}
}

func TestCompactNoLogReturnsErrNoLog(t *testing.T) {
	s := New()
	if err := s.Compact(); err != ErrNoLog {
		t.Fatalf("got %v, want ErrNoLog", err)
	}
}

func logSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func TestAutoCompactTriggersOnThreshold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	for i := 0; i < compactThresholdMinWrites+1; i++ {
		if err := s.Set("k", "v"); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	size, err := logSize(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	// Without auto-compaction the log would hold one record per write. If
	// it stayed near the size of a single record, compaction kicked in.
	single := int64(len(encodeSet("k", "v")))
	if size > single*10 {
		t.Fatalf("expected auto-compaction to keep log near %d bytes, got %d", single, size)
	}

	if v, err := s.Get("k"); err != nil || v != "v" {
		t.Fatalf("got (%q, %v), want (\"v\", nil)", v, err)
	}
}

func TestOpenCompactsStaleLogOnStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.log")

	var raw []byte
	for i := 0; i < compactThresholdMinWrites+10; i++ {
		raw = append(raw, encodeSet("k", "v")...)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("failed to write fixture: %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	size, err := logSize(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	single := int64(len(encodeSet("k", "v")))
	if size > single*10 {
		t.Fatalf("expected Open to compact stale log down to ~%d bytes, got %d", single, size)
	}

	if v, err := s.Get("k"); err != nil || v != "v" {
		t.Fatalf("got (%q, %v), want (\"v\", nil)", v, err)
	}
}
