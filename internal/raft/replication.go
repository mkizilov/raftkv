package raft

import (
	"context"
	"fmt"

	"github.com/you/raftkv/internal/transport"
)

func (s *nodeState) termAt(index uint64) uint64 {
	if index == s.snapshot.Index { return s.snapshot.Term }
	return s.log[index-s.snapshot.Index-1].Term
}

func (s *nodeState) lastIndex() uint64 { return s.snapshot.Index + uint64(len(s.log)) }

const maxAppendEntries = 64
const maxAppendBytes = 1 << 20

// appendBatch groups queued entries while bounding ordinary RPC payloads. A
// single large entry is sent intact because entries cannot be split by Raft.
func appendBatch(entries []transport.LogEntry) []transport.LogEntry {
	count, size := 0, 0
	for _, entry := range entries {
		entrySize := len(entry.Command) + 32
		if count > 0 && (count >= maxAppendEntries || entrySize > maxAppendBytes-size) { break }
		count++
		size += entrySize
		if size >= maxAppendBytes { break }
	}
	return cloneLog(entries[:count])
}

func (s *nodeState) saveLog(entries []transport.LogEntry) bool {
	if err := s.node.config.Storage.SaveLog(entries); err != nil {
		s.err = fmt.Errorf("raft: persist log: %w", err)
		return false
	}
	s.log = entries
	return true
}

type startResult struct {
	index, term uint64
	leader bool
}

// Start appends a command on a leader. A successful return does not mean that
// the entry has committed. Storage errors and stopped nodes return isLeader=false.
func (n *Node) Start(command []byte) (index, term uint64, isLeader bool) {
	index, term, isLeader, _ = n.Propose(context.Background(), command)
	return
}

// Propose is the context-aware form of Start, including storage errors. If the
// context expires, the proposal may nevertheless have been appended.
func (n *Node) Propose(ctx context.Context, command []byte) (index, term uint64, isLeader bool, err error) {
	command = append([]byte(nil), command...)
	r, err := call(ctx, n, func(s *nodeState) (startResult, error) {
		result := startResult{term: s.hard.Term}
		if s.role != Leader { return result, nil }
		if s.lastIndex() >= ^uint64(0)-1 { return result, ErrLogExhausted }
		entries := cloneLog(s.log)
		entries = append(entries, transport.LogEntry{Term: s.hard.Term, Command: command})
		if !s.saveLog(entries) { return result, s.err }
		result.index, result.leader = s.lastIndex(), true
		s.advanceCommit()
		s.heartbeats()
		return result, nil
	})
	return r.index, r.term, r.leader, err
}

func (s *nodeState) replicate(peer transport.NodeID) {
	if s.inflight[peer] { return }
	next := s.nextIndex[peer]
	if next <= s.snapshot.Index { s.sendSnapshot(peer); return }
	prev := next - 1
	req := transport.AppendEntriesRequest{
		Term: s.hard.Term, LeaderID: s.node.config.ID,
		PrevLogIndex: prev, PrevLogTerm: s.termAt(prev),
		Entries: appendBatch(s.log[prev-s.snapshot.Index:]), LeaderCommit: s.commitIndex,
	}
	s.inflight[peer] = true
	s.node.spawn(func(ctx context.Context) {
		response, err := s.node.config.Transport.AppendEntries(ctx, peer, req)
		s.node.post(func(state *nodeState) {
			if err == nil && !state.observeTerm(response.Term) { return }
			if state.role != Leader || state.hard.Term != req.Term { return }
			state.inflight[peer] = false
			if err != nil { return }
			if !response.Success {
				state.nextIndex[peer] = state.backtrack(state.nextIndex[peer], response)
				state.replicate(peer)
				return
			}
			matched := req.PrevLogIndex + uint64(len(req.Entries))
			state.matchIndex[peer] = matched
			state.nextIndex[peer] = matched + 1
			state.advanceCommit()
			if matched < state.lastIndex() { state.replicate(peer) }
		})
	})
}

// backtrack finds the leader's last entry in the conflicting term, or uses the
// follower's first index in that term (or its end + 1 if it is too short).
func (s *nodeState) backtrack(next uint64, response transport.AppendEntriesResponse) uint64 {
	if response.Compacted && response.ConflictIndex > next && response.ConflictIndex <= s.lastIndex()+1 { return response.ConflictIndex }
	if next <= 1 { return 1 }
	target := response.ConflictIndex
	if response.HasConflictTerm {
		for i := s.lastIndex(); i > s.snapshot.Index; i-- {
			if s.termAt(i) == response.ConflictTerm { target = i + 1; break }
		}
		if target == response.ConflictIndex && s.snapshot.Index > 0 && s.snapshot.Term == response.ConflictTerm { target = s.snapshot.Index + 1 }
	}
	if target == 0 || target >= next { return next - 1 }
	return target
}

func (s *nodeState) advanceCommit() {
	for index := s.lastIndex(); index > s.commitIndex; index-- {
		if s.termAt(index) != s.hard.Term { continue }
		count := 1
		for _, peer := range s.node.config.Peers {
			if s.matchIndex[peer] >= index { count++ }
		}
		if count > (len(s.node.config.Peers)+1)/2 {
			s.commitIndex = index
			return
		}
	}
}
