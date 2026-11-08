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

// Storage must make Save durable before returning successfully.
type Storage interface {
	Load() (HardState, error)
	Save(HardState) error
}

// MemoryStorage survives node restarts when the same instance is reused.
type MemoryStorage struct {
	mu sync.Mutex
	state HardState
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
