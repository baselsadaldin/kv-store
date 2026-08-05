package store

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Open opens (creating if necessary) a Store backed by a write-ahead log at path.
// Any records already in the log are replayed to rebuild the in-memory state.
// If the log ends in a torn write (e.g. from a crash mid-write), the incomplete
// trailing record is discarded and the file is truncated to the last valid record.
// If the replayed log already holds far more records than live keys (see
// maybeCompactLocked), it is compacted immediately before Open returns.
func Open(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	raw, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	data, validLen, recordCount := replay(raw)

	if validLen < int64(len(raw)) {
		if err := f.Truncate(validLen); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}

	s := &Store{data: data, log: f, writesSinceCompact: recordCount}
	if err := s.maybeCompactLocked(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the store's underlying log file. It is a no-op for stores
// created with New() that have no log.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return nil
	}
	return s.log.Close()
}

// Compact rewrites the log to contain only the records needed to reproduce the
// store's current state, discarding overwritten and deleted entries. It
// returns ErrNoLog for a store created with New. Set and Delete also trigger
// this automatically once the log has accumulated enough dead records (see
// maybeCompactLocked); call Compact directly to force it immediately.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return ErrNoLog
	}
	return s.compactLocked()
}

// compactThresholdMinWrites is the minimum number of records a log must
// accumulate before auto-compaction considers rewriting it, so small stores
// aren't compacted on every other write.
const compactThresholdMinWrites = 100

// compactThresholdGrowthFactor bounds the ratio of on-disk records to live
// keys: once the log holds more than this many records per live key (and at
// least compactThresholdMinWrites total), it's due for a rewrite.
const compactThresholdGrowthFactor = 4

// maybeCompactLocked compacts the log if it has grown large relative to the
// live key count. The caller must already hold s.mu. It is a no-op for a
// store with no backing log.
func (s *Store) maybeCompactLocked() error {
	if s.log == nil {
		return nil
	}
	if s.writesSinceCompact < compactThresholdMinWrites {
		return nil
	}
	if s.writesSinceCompact < compactThresholdGrowthFactor*len(s.data) {
		return nil
	}
	return s.compactLocked()
}

// compactLocked does the actual rewrite. The caller must already hold s.mu
// and have confirmed s.log is non-nil. The rewrite is done via a temp file
// and atomic rename so a crash mid-compaction can't corrupt or lose the
// existing log.
func (s *Store) compactLocked() error {
	path := s.log.Name()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".kvstore-compact-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	for k, v := range s.data {
		if _, err := tmp.Write(encodeSet(k, v)); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}
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

	if err := s.log.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	s.log = f
	s.writesSinceCompact = 0
	return nil
}

// appendRecord writes record to the log and syncs it to disk before returning,
// so a write is only considered complete once it is durable.
func appendRecord(f *os.File, record []byte) error {
	if _, err := f.Write(record); err != nil {
		return err
	}
	return f.Sync()
}

func encodeSet(key, value string) []byte {
	header := fmt.Sprintf("SET %d %d\n", len(key), len(value))
	buf := make([]byte, 0, len(header)+len(key)+len(value))
	buf = append(buf, header...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	return buf
}

func encodeDelete(key string) []byte {
	header := fmt.Sprintf("DEL %d\n", len(key))
	buf := make([]byte, 0, len(header)+len(key))
	buf = append(buf, header...)
	buf = append(buf, key...)
	return buf
}

// replay parses raw log bytes into a map, applying records in order. It returns
// the map, the byte offset up to which records were fully valid, and the
// number of records applied. If the log ends in a torn or unrecognized
// record, replay stops there rather than erroring, treating the remainder as
// a crash artifact to be discarded by the caller.
func replay(raw []byte) (map[string]string, int64, int) {
	data := make(map[string]string)
	var offset int64
	var count int

	for offset < int64(len(raw)) {
		rest := raw[offset:]
		nl := bytes.IndexByte(rest, '\n')
		if nl == -1 {
			break
		}
		fields := strings.Fields(string(rest[:nl]))

		switch {
		case len(fields) == 3 && fields[0] == "SET":
			keyLen, err1 := strconv.Atoi(fields[1])
			valLen, err2 := strconv.Atoi(fields[2])
			if err1 != nil || err2 != nil || keyLen < 0 || valLen < 0 {
				return data, offset, count
			}
			need := nl + 1 + keyLen + valLen
			if need > len(rest) {
				return data, offset, count
			}
			payload := rest[nl+1 : need]
			data[string(payload[:keyLen])] = string(payload[keyLen:])
			offset += int64(need)
			count++

		case len(fields) == 2 && fields[0] == "DEL":
			keyLen, err1 := strconv.Atoi(fields[1])
			if err1 != nil || keyLen < 0 {
				return data, offset, count
			}
			need := nl + 1 + keyLen
			if need > len(rest) {
				return data, offset, count
			}
			delete(data, string(rest[nl+1:need]))
			offset += int64(need)
			count++

		default:
			return data, offset, count
		}
	}

	return data, offset, count
}
