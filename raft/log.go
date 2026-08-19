package raft

// Command is a state-machine operation carried by a log entry. It mirrors
// the operations store.Store already supports; Op is "SET" or "DEL".
type Command struct {
	Op    string
	Key   string
	Value string // unused for "DEL"
}

// Entry is one record in a Raft log: a command tagged with the term it was
// proposed in, at a specific log position.
type Entry struct {
	Term    int
	Index   int
	Command Command
}

// Log is a node's Raft log: an ordered, 1-indexed list of entries. It is
// in-memory only for now; persistence is a separate follow-up.
type Log struct {
	entries []Entry // entries[i] has Index == i+1
}

// NewLog returns an empty Log.
func NewLog() *Log {
	return &Log{}
}

// Append adds a new entry for command at the next index, tagged with term,
// and returns it.
func (l *Log) Append(term int, command Command) Entry {
	e := Entry{Term: term, Index: len(l.entries) + 1, Command: command}
	l.entries = append(l.entries, e)
	return e
}

// Get returns the entry at index, and whether it exists.
func (l *Log) Get(index int) (Entry, bool) {
	if index < 1 || index > len(l.entries) {
		return Entry{}, false
	}
	return l.entries[index-1], true
}

// LastIndex returns the index of the last entry, or 0 if the log is empty.
func (l *Log) LastIndex() int {
	return len(l.entries)
}

// LastTerm returns the term of the last entry, or 0 if the log is empty.
func (l *Log) LastTerm() int {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// TruncateFrom removes the entry at index and every entry after it. It is
// used when a Follower's log conflicts with the Leader's and must be
// overwritten (see AppendEntries handling). It is a no-op if index is
// already past the end of the log.
func (l *Log) TruncateFrom(index int) {
	if index < 1 {
		index = 1
	}
	if index-1 < len(l.entries) {
		l.entries = l.entries[:index-1]
	}
}
