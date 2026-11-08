package raft

import (
	"context"
	"fmt"

	"github.com/you/raftkv/internal/transport"
)

func (s *nodeState) termAt(index uint64) uint64 {
	if index == 0 { return 0 }
	return s.log[index-1].Term
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
		entries := cloneLog(s.log)
		entries = append(entries, transport.LogEntry{Term: s.hard.Term, Command: command})
		if !s.saveLog(entries) { return result, s.err }
		result.index, result.leader = uint64(len(s.log)), true
		s.advanceCommit()
		s.heartbeats()
		return result, nil
	})
	return r.index, r.term, r.leader, err
}

func (s *nodeState) replicate(peer transport.NodeID) {
	if s.inflight[peer] { return }
	next := s.nextIndex[peer]
	prev := next - 1
	req := transport.AppendEntriesRequest{
		Term: s.hard.Term, LeaderID: s.node.config.ID,
		PrevLogIndex: prev, PrevLogTerm: s.termAt(prev),
		Entries: cloneLog(s.log[prev:]), LeaderCommit: s.commitIndex,
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
				if state.nextIndex[peer] > 1 { state.nextIndex[peer]-- }
				state.replicate(peer)
				return
			}
			matched := req.PrevLogIndex + uint64(len(req.Entries))
			state.matchIndex[peer] = matched
			state.nextIndex[peer] = matched + 1
			state.advanceCommit()
			if matched < uint64(len(state.log)) { state.replicate(peer) }
		})
	})
}

func (s *nodeState) advanceCommit() {
	for index := uint64(len(s.log)); index > s.commitIndex; index-- {
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
