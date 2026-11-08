package raft

import (
	"context"
	"errors"

	"github.com/you/raftkv/internal/transport"
)

var ErrNotLeader = errors.New("raft: not leader")
var ErrReadNotReady = errors.New("raft: no entry from the current term is committed")

type readBarrier struct { index, term uint64 }

// ReadIndex confirms leadership with a fresh quorum round. The caller must wait
// until its state machine has applied at least the returned index before reading.
// The leader must first commit an entry in its own term; otherwise callers may
// retry after normal replication establishes that condition.
func (n *Node) ReadIndex(ctx context.Context) (uint64, uint64, error) {
	return n.confirmLeader(ctx, true)
}

// ConfirmLeader establishes current quorum contact, but does not authorize a
// state read: unlike ReadIndex it does not require a current-term commit.
func (n *Node) ConfirmLeader(ctx context.Context) (uint64, uint64, error) {
	return n.confirmLeader(ctx, false)
}

func (n *Node) confirmLeader(ctx context.Context, requireCommit bool) (uint64, uint64, error) {
	reply := make(chan result[readBarrier], 1)
	_, err := call(ctx, n, func(s *nodeState) (struct{}, error) {
		if s.role != Leader { return struct{}{}, ErrNotLeader }
		if requireCommit && (s.commitIndex == 0 || s.termAt(s.commitIndex) != s.hard.Term) { return struct{}{}, ErrReadNotReady }
		barrier := readBarrier{index: s.commitIndex, term: s.hard.Term}
		votes := 1
		if votes > (len(n.config.Peers)+1)/2 { reply <- result[readBarrier]{value: barrier}; return struct{}{}, nil }
		req := transport.AppendEntriesRequest{Term: barrier.term, LeaderID: n.config.ID, PrevLogIndex: s.snapshot.Index, PrevLogTerm: s.snapshot.Term}
		for _, peer := range n.config.Peers {
			peer := peer
			n.spawn(func(rpcCtx context.Context) {
				roundCtx, cancel := context.WithCancel(rpcCtx)
				stop := context.AfterFunc(ctx, cancel)
				defer stop()
				defer cancel()
				response, err := n.config.Transport.AppendEntries(roundCtx, peer, req)
				if err != nil { return }
				n.post(func(state *nodeState) {
					if !state.observeTerm(response.Term) { return }
					if state.role != Leader || state.hard.Term != barrier.term {
						select { case reply <- result[readBarrier]{err: ErrNotLeader}: default: }
						return
					}
					if response.Term != barrier.term { return }
					votes++
					if votes > (len(n.config.Peers)+1)/2 { select { case reply <- result[readBarrier]{value: barrier}: default: } }
				})
			})
		}
		return struct{}{}, nil
	})
	if err != nil { return 0, 0, err }
	select {
	case r := <-reply: return r.value.index, r.value.term, r.err
	case <-ctx.Done(): return 0, 0, ctx.Err()
	case <-n.done: return 0, 0, ErrStopped
	}
}
