package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/you/raftkv/internal/transport"
)

// Snapshot discards entries up to an already applied index. The service must
// supply its complete state after applying that index.
func (n *Node) Snapshot(index uint64, data []byte) error {
	data = append([]byte(nil), data...)
	_, err := call(context.Background(), n, func(s *nodeState) (struct{}, error) {
		if index <= s.snapshot.Index || index > s.lastApplied || s.pendingSnapshot != nil { return struct{}{}, errors.New("raft: snapshot index must be applied and newer than the current snapshot") }
		snapshot := SnapshotState{Index: index, Term: s.termAt(index), Data: data}
		suffix := cloneLog(s.log[index-s.snapshot.Index:])
		if err := s.node.config.Storage.SaveSnapshot(snapshot, suffix); err != nil {
			s.err = fmt.Errorf("raft: persist snapshot: %w", err)
			return struct{}{}, s.err
		}
		s.snapshot, s.log = snapshot, suffix
		return struct{}{}, nil
	})
	return err
}

func (s *nodeState) sendSnapshot(peer transport.NodeID) {
	snapshot := cloneSnapshot(s.snapshot)
	req := transport.InstallSnapshotRequest{Term: s.hard.Term, LeaderID: s.node.config.ID, LastIncludedIndex: snapshot.Index, LastIncludedTerm: snapshot.Term, Data: snapshot.Data, Done: true}
	s.inflight[peer] = true
	s.node.spawn(func(ctx context.Context) {
		response, err := s.node.config.Transport.InstallSnapshot(ctx, peer, req)
		s.node.post(func(state *nodeState) {
			if err == nil && !state.observeTerm(response.Term) { return }
			if state.role != Leader || state.hard.Term != req.Term { return }
			state.inflight[peer] = false
			if err != nil { return }
			if state.matchIndex[peer] < req.LastIncludedIndex { state.matchIndex[peer] = req.LastIncludedIndex }
			state.nextIndex[peer] = state.matchIndex[peer] + 1
			state.replicate(peer)
		})
	})
}

func (n *Node) InstallSnapshot(ctx context.Context, req transport.InstallSnapshotRequest) (transport.InstallSnapshotResponse, error) {
	req.Data = append([]byte(nil), req.Data...)
	return call(ctx, n, func(s *nodeState) (transport.InstallSnapshotResponse, error) {
		if !s.observeTerm(req.Term) { return transport.InstallSnapshotResponse{}, s.err }
		response := transport.InstallSnapshotResponse{Term: s.hard.Term}
		if req.Term < s.hard.Term { return response, nil }
		s.role = Follower
		s.votes = nil
		s.reset(s.electionTimeout())
		if !req.Done || req.Offset != 0 || req.LastIncludedIndex == 0 { return response, errors.New("raft: invalid or unsupported snapshot chunk") }
		if req.LastIncludedIndex <= s.commitIndex { return response, nil }
		var suffix []transport.LogEntry
		if req.LastIncludedIndex <= s.lastIndex() && s.termAt(req.LastIncludedIndex) == req.LastIncludedTerm {
			suffix = cloneLog(s.log[req.LastIncludedIndex-s.snapshot.Index:])
		}
		snapshot := SnapshotState{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm, Data: req.Data}
		if err := s.node.config.Storage.SaveSnapshot(snapshot, suffix); err != nil {
			s.err = fmt.Errorf("raft: persist snapshot: %w", err)
			return response, s.err
		}
		s.snapshot, s.log = snapshot, suffix
		s.commitIndex, s.lastApplied = snapshot.Index, snapshot.Index
		pending := cloneSnapshot(snapshot)
		s.pendingSnapshot = &pending
		return response, nil
	})
}
