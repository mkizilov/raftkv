package raft

import (
	"sync"

	"github.com/you/raftkv/internal/transport"
)

// HardState is persisted atomically before a term or vote becomes observable.
type HardState struct {
	Term uint64
	VotedFor transport.NodeID
	HasVote bool
}

// Storage must atomically replace the log in SaveLog and make both Save and
// SaveLog durable before returning successfully. Callers own returned slices.
type Storage interface {
	Load() (HardState, error)
	Save(HardState) error
	LoadLog() ([]transport.LogEntry, error)
	SaveLog([]transport.LogEntry) error
}

// MemoryStorage survives node restarts when the same instance is reused.
type MemoryStorage struct {
	mu sync.Mutex
	state HardState
	log []transport.LogEntry
}

func cloneLog(entries []transport.LogEntry) []transport.LogEntry {
	copyEntries := make([]transport.LogEntry, len(entries))
	for i, entry := range entries {
		copyEntries[i] = transport.LogEntry{Term: entry.Term, Command: append([]byte(nil), entry.Command...)}
	}
	return copyEntries
}

func (s *MemoryStorage) LoadLog() ([]transport.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneLog(s.log), nil
}

func (s *MemoryStorage) SaveLog(entries []transport.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = cloneLog(entries)
	return nil
}

func (s *MemoryStorage) Load() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *MemoryStorage) Save(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}
