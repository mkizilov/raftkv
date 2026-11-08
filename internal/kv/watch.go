package kv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/you/raftkv/internal/raft"
)

const watchHistoryLimit = 128
const watchHistoryByteLimit = 1 << 20

var ErrWatchCompacted = errors.New("kv: watch history compacted; re-read full state")
var ErrWatchQuorum = errors.New("kv: watch lost quorum")

type CompactedError struct { Revision uint64 }
func (e *CompactedError) Error() string { return fmt.Sprintf("%s (compacted through revision %d)", ErrWatchCompacted, e.Revision) }
func (e *CompactedError) Unwrap() error { return ErrWatchCompacted }

type WatchEvent struct {
	Revision uint64
	Key, Value string
	Version uint64
	Deleted bool
}

type watchQuery struct { prefix string; after, barrier uint64; reply chan watchResult }
type watchResult struct { event WatchEvent; found bool; scanned uint64; err error }

func (s *Server) recordWatch(event WatchEvent) {
	size := watchEventSize(event)
	if size > watchHistoryByteLimit {
		s.state.Events = nil
		s.state.CompactedRevision = event.Revision
		return
	}
	used := 0
	for _, old := range s.state.Events { used += watchEventSize(old) }
	for len(s.state.Events) > 0 && (len(s.state.Events) >= watchHistoryLimit || used+size > watchHistoryByteLimit) {
		used -= watchEventSize(s.state.Events[0])
		s.state.CompactedRevision = s.state.Events[0].Revision
		copy(s.state.Events, s.state.Events[1:])
		s.state.Events = s.state.Events[:len(s.state.Events)-1]
	}
	s.state.Events = append(s.state.Events, event)
}

func watchEventSize(event WatchEvent) int {
	if len(event.Key) > watchHistoryByteLimit || len(event.Value) > watchHistoryByteLimit { return watchHistoryByteLimit + 1 }
	return len(event.Key) + len(event.Value) + 32
}

func (s *Server) nextWatch(prefix string, after uint64) watchResult {
	if after < s.state.CompactedRevision { return watchResult{err: &CompactedError{Revision: s.state.CompactedRevision}} }
	for _, event := range s.state.Events {
		if event.Revision > after && strings.HasPrefix(event.Key, prefix) { return watchResult{event: event, found: true} }
	}
	if after < s.state.Index { after = s.state.Index }
	return watchResult{scanned: after}
}

// A newly elected leader must commit an entry from its term before it knows
// the full committed prefix. The barrier has no user-visible state effect.
func (s *Server) watchStart(ctx context.Context) (uint64, uint64, error) {
	index, term, err := s.node.ReadIndex(ctx)
	if !errors.Is(err, raft.ErrReadNotReady) { return index, term, err }
	_, _, leader, err := s.node.Propose(ctx, []byte(`{"Operation":"watch_barrier"}`))
	if err != nil { return 0, 0, err }
	if !leader { return 0, 0, raft.ErrNotLeader }
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		index, term, err = s.node.ReadIndex(ctx)
		if !errors.Is(err, raft.ErrReadNotReady) { return index, term, err }
		select { case <-ticker.C: case <-ctx.Done(): return 0, 0, ctx.Err() }
	}
}

// Watch delivers committed events strictly after afterRevision, in order.
// send is called serially without holding a server or Raft lock. A slow caller
// gets ErrWatchCompacted if its cursor falls behind the bounded history.
func (s *Server) Watch(ctx context.Context, prefix string, afterRevision uint64, send func(WatchEvent) error) error {
	if send == nil { return errors.New("kv: watch callback is required") }
	confirm := func() (uint64, uint64, error) {
		check, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		index, term, err := s.node.ConfirmLeader(check)
		if errors.Is(err, raft.ErrNotLeader) { return 0, 0, ErrNotLeader }
		if err != nil && ctx.Err() != nil { return 0, 0, ctx.Err() }
		if errors.Is(err, context.DeadlineExceeded) { return 0, 0, ErrWatchQuorum }
		return index, term, err
	}
	initial, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	barrier, term, err := s.watchStart(initial)
	stop()
	if errors.Is(err, raft.ErrNotLeader) { return ErrNotLeader }
	if err != nil && ctx.Err() != nil { return ctx.Err() }
	if errors.Is(err, context.DeadlineExceeded) { return ErrWatchQuorum }
	if err != nil { return err }
	checkAt := time.Now().Add(250 * time.Millisecond)
	for {
		if !time.Now().Before(checkAt) {
			nextBarrier, nextTerm, err := confirm()
			if err != nil { return err }
			if nextTerm != term { return ErrNotLeader }
			barrier = nextBarrier
			checkAt = time.Now().Add(250 * time.Millisecond)
		}
		s.mu.Lock()
		changed := s.applied
		s.mu.Unlock()
		reply := make(chan watchResult, 1)
		select {
		case s.watches <- watchQuery{prefix: prefix, after: afterRevision, barrier: barrier, reply: reply}:
		case <-ctx.Done(): return ctx.Err()
		case <-s.done: return s.failure()
		}
		var result watchResult
		select {
		case result = <-reply:
		case <-ctx.Done(): return ctx.Err()
		case <-s.done: return s.failure()
		}
		if result.err != nil { return result.err }
		if result.found {
			if err := send(result.event); err != nil { return err }
			afterRevision = result.event.Revision
			continue
		}
		afterRevision = result.scanned
		timer := time.NewTimer(time.Until(checkAt))
		select {
		case <-changed: timer.Stop()
		case <-timer.C:
		case <-ctx.Done(): timer.Stop(); return ctx.Err()
		case <-s.done: timer.Stop(); return s.failure()
		}
	}
}
