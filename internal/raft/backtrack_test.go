package raft

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

type countedHandler struct { *Node; calls atomic.Int64 }

func (h *countedHandler) AppendEntries(ctx context.Context, req transport.AppendEntriesRequest) (transport.AppendEntriesResponse, error) {
	h.calls.Add(1)
	return h.Node.AppendEntries(ctx, req)
}

func TestFastBacktrackingThousands(t *testing.T) {
	for _, followerLength := range []int{1000, 4000} {
		t.Run(fmt.Sprint(followerLength), func(t *testing.T) {
			network := transport.NewNetwork(1)
			leaderLog := make([]transport.LogEntry, 5000)
			for i := range leaderLog {
				term := uint64(1)
				if i >= 1000 { term = 2 }
				if i >= 3000 { term = 3 }
				leaderLog[i] = transport.LogEntry{Term: term, Command: []byte(fmt.Sprint(i))}
			}
			followerLog := cloneLog(leaderLog[:followerLength])
			for i := 3000; i < followerLength; i++ { followerLog[i].Term = 2 }
			leaderStore, followerStore := &MemoryStorage{}, &MemoryStorage{}
			if err := leaderStore.SaveLog(leaderLog); err != nil { t.Fatal(err) }
			if err := followerStore.SaveLog(followerLog); err != nil { t.Fatal(err) }
			config := func(id transport.NodeID, store Storage, peers []transport.NodeID) Config {
				return Config{ID: id, Peers: peers, Storage: store, Transport: network.Endpoint(id), ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute}
			}
			leader, err := NewNode(config(1, leaderStore, []transport.NodeID{2}))
			if err != nil { t.Fatal(err) }
			follower, err := NewNode(config(2, followerStore, []transport.NodeID{1}))
			if err != nil { t.Fatal(err) }
			h := &countedHandler{Node: follower}
			network.Register(1, leader)
			network.Register(2, h)
			leader.Run()
			follower.Run()
			defer leader.Stop()
			defer follower.Stop()
			_, err = call(context.Background(), leader, func(s *nodeState) (struct{}, error) {
				if !s.persist(HardState{Term: 9}) { return struct{}{}, s.err }
				s.becomeLeader()
				return struct{}{}, nil
			})
			if err != nil { t.Fatal(err) }
			deadline := time.Now().Add(3 * time.Second)
			for {
				entries, err := followerStore.LoadLog()
				if err != nil { t.Fatal(err) }
				if reflect.DeepEqual(entries, leaderLog) { break }
				if time.Now().After(deadline) { t.Fatal("follower did not catch up") }
				time.Sleep(time.Millisecond)
			}
			if calls := h.calls.Load(); calls > 3 { t.Fatalf("used %d AppendEntries calls for %d entries", calls, followerLength) }
		})
	}
}

func TestBacktrackHints(t *testing.T) {
	s := nodeState{log: []transport.LogEntry{{Term: 1}, {Term: 2}, {Term: 2}, {Term: 3}}}
	tests := []struct { next uint64; response transport.AppendEntriesResponse; want uint64 }{
		{5, transport.AppendEntriesResponse{ConflictIndex: 2, ConflictTerm: 2, HasConflictTerm: true}, 4},
		{5, transport.AppendEntriesResponse{ConflictIndex: 2, ConflictTerm: 9, HasConflictTerm: true}, 2},
		{5, transport.AppendEntriesResponse{ConflictIndex: 2}, 2},
		{3, transport.AppendEntriesResponse{ConflictIndex: 1, ConflictTerm: 3, HasConflictTerm: true}, 2},
		{3, transport.AppendEntriesResponse{}, 2},
		{1, transport.AppendEntriesResponse{ConflictIndex: 1}, 1},
		{1, transport.AppendEntriesResponse{ConflictIndex: 4, Compacted: true}, 4},
	}
	for _, test := range tests {
		if got := s.backtrack(test.next, test.response); got != test.want { t.Fatalf("backtrack(%d, %+v) = %d, want %d", test.next, test.response, got, test.want) }
	}
}
