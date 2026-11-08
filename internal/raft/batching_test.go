package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

type batchHandler struct {
	first chan struct{}
	release chan struct{}
	requests chan transport.AppendEntriesRequest
}

func (h *batchHandler) RequestVote(context.Context, transport.RequestVoteRequest) (transport.RequestVoteResponse, error) {
	return transport.RequestVoteResponse{}, nil
}
func (h *batchHandler) AppendEntries(ctx context.Context, req transport.AppendEntriesRequest) (transport.AppendEntriesResponse, error) {
	select {
	case h.first <- struct{}{}:
		select { case <-h.release: case <-ctx.Done(): return transport.AppendEntriesResponse{}, ctx.Err() }
	default:
	}
	select { case h.requests <- req: case <-ctx.Done(): return transport.AppendEntriesResponse{}, ctx.Err() }
	return transport.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}
func (h *batchHandler) InstallSnapshot(context.Context, transport.InstallSnapshotRequest) (transport.InstallSnapshotResponse, error) {
	return transport.InstallSnapshotResponse{}, nil
}

func TestQueuedCommandsShareAppendEntries(t *testing.T) {
	network := transport.NewNetwork(1)
	handler := &batchHandler{first: make(chan struct{}, 1), release: make(chan struct{}), requests: make(chan transport.AppendEntriesRequest, 8)}
	node, err := NewNode(Config{ID: 1, Peers: []transport.NodeID{2}, Transport: network.Endpoint(1), Storage: &MemoryStorage{}, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute})
	if err != nil { t.Fatal(err) }
	network.Register(1, node)
	network.Register(2, handler)
	node.Run()
	defer node.Stop()
	_, err = call(context.Background(), node, func(s *nodeState) (struct{}, error) {
		if !s.persist(HardState{Term: 1}) { return struct{}{}, s.err }
		s.becomeLeader()
		return struct{}{}, nil
	})
	if err != nil { t.Fatal(err) }
	select { case <-handler.first: case <-time.After(time.Second): t.Fatal("initial heartbeat not received") }
	for i := 0; i < 5; i++ {
		if _, _, ok := node.Start([]byte(fmt.Sprint(i))); !ok { t.Fatal("leader rejected proposal") }
	}
	close(handler.release)
	select { case <-handler.requests: case <-time.After(time.Second): t.Fatal("heartbeat did not complete") }
	select {
	case request := <-handler.requests:
		if len(request.Entries) != 5 { t.Fatalf("expected five queued entries, got %d", len(request.Entries)) }
		for i, entry := range request.Entries { if string(entry.Command) != fmt.Sprint(i) { t.Fatalf("entry %d = %q", i, entry.Command) } }
	case <-time.After(time.Second): t.Fatal("batched append not received")
	}
}

func TestAppendBatchLimits(t *testing.T) {
	entries := make([]transport.LogEntry, maxAppendEntries+10)
	if got := len(appendBatch(entries)); got != maxAppendEntries { t.Fatalf("entry limit = %d", got) }
	large := []transport.LogEntry{{Command: make([]byte, maxAppendBytes)}, {Command: []byte("next")}}
	if got := len(appendBatch(large)); got != 1 { t.Fatalf("large entry batch = %d", got) }
}
