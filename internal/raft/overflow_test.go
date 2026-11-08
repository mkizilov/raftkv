package raft

import (
	"context"
	"errors"
	"testing"

	"github.com/you/raftkv/internal/transport"
)

func TestTermAndLogIndexExhaustionFailClosed(t *testing.T) {
	state := &nodeState{hard: HardState{Term: ^uint64(0)}}
	state.elect()
	if state.err == nil { t.Fatal("term wrapped instead of failing") }
	store := &MemoryStorage{}
	if err := store.SaveSnapshot(SnapshotState{Index: ^uint64(0), Term: 1}, nil); err != nil { t.Fatal(err) }
	network := transport.NewNetwork(1)
	if _, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: store}); !errors.Is(err, ErrLogExhausted) { t.Fatalf("overflowing snapshot: %v", err) }
	store = &MemoryStorage{}
	node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: store})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	_, err = node.InstallSnapshot(context.Background(), transport.InstallSnapshotRequest{Term: 1, LastIncludedIndex: ^uint64(0), Done: true})
	if err == nil { t.Fatal("accepted unrepresentable snapshot index") }
}
