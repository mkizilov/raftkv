package raft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

func TestCanceledQueuedProposalIsNotAppended(t *testing.T) {
	network := transport.NewNetwork(1)
	node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: &MemoryStorage{}, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	_, err = call(context.Background(), node, func(s *nodeState) (struct{}, error) { s.elect(); return struct{}{}, s.err })
	if err != nil { t.Fatal(err) }
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	node.post(func(*nodeState) { close(entered); <-release })
	select { case <-entered: case <-time.After(time.Second): t.Fatal("state owner did not pause") }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, _, err := node.Propose(ctx, []byte("canceled")); done <- err }()
	deadline := time.Now().Add(time.Second)
	for len(node.events) == 0 && time.Now().Before(deadline) { time.Sleep(time.Millisecond) }
	if len(node.events) == 0 { t.Fatal("proposal was not queued") }
	cancel()
	select {
	case err := <-done: if !errors.Is(err, context.Canceled) { t.Fatalf("proposal error: %v", err) }
	case <-time.After(time.Second): t.Fatal("canceled proposal did not return")
	}
	releaseOnce.Do(func() { close(release) })
	index, err := call(context.Background(), node, func(s *nodeState) (uint64, error) { return s.lastIndex(), nil })
	if err != nil || index != 0 { t.Fatalf("canceled proposal appended at index %d: %v", index, err) }
}
