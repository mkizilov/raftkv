package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeKV struct {
	pb.UnimplementedKVServiceServer
	mu sync.Mutex
	redirect string
	block bool
	requests map[RequestID]*pb.KVResponse
	writes int
	attempts int
}

func (s *fakeKV) Put(ctx context.Context, req *pb.PutRequest) (*pb.KVResponse, error) {
	if s.block { <-ctx.Done(); return nil, ctx.Err() }
	if s.redirect != "" {
		st, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&pb.NotLeaderDetail{LeaderAddress: s.redirect})
		if err != nil { return nil, err }
		return nil, st.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := RequestID{ClientID: req.Identity.ClientId, Sequence: req.Identity.Sequence}
	s.attempts++
	if saved := s.requests[id]; saved != nil { return saved, nil }
	s.writes++
	result := &pb.KVResponse{Value: req.Value, Version: uint64(s.writes), Found: true}
	s.requests[id] = result
	return nil, status.Error(codes.Unavailable, "response lost after commit")
}

func (s *fakeKV) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{NodeId: 2, Role: "leader", Term: 4, CommitIndex: 9, LeaderKnown: true, LeaderId: 2, LeaderAddress: "127.0.0.1:21877"}, nil
}

func startFake(t *testing.T, address string, handler *fakeKV) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil { t.Fatal(err) }
	server := grpc.NewServer()
	pb.RegisterKVServiceServer(server, handler)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); if err := <-done; err != nil { t.Error(err) } })
}

func TestRedirectAndLostResponseRetry(t *testing.T) {
	leader := &fakeKV{requests: make(map[RequestID]*pb.KVResponse)}
	startFake(t, "127.0.0.1:21876", &fakeKV{redirect: "127.0.0.1:21877"})
	startFake(t, "127.0.0.1:21877", leader)
	c, err := New(Config{Endpoints: []string{"127.0.0.1:21876", "127.0.0.1:21877"}, ClientID: "test"})
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := c.Close(); err != nil { t.Error(err) } })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	id, err := c.NewRequest()
	if err != nil { t.Fatal(err) }
	result, err := c.PutWithID(ctx, id, "key", "value")
	if err != nil || result.Version != 1 || result.Value != "value" { t.Fatalf("put: %+v %v", result, err) }
	retry, err := c.PutWithID(ctx, id, "key", "value")
	if err != nil || retry != result { t.Fatalf("retry: %+v %v", retry, err) }
	states, err := c.Status(ctx)
	if err != nil || len(states) != 2 || states[1].NodeID != 2 || states[1].Term != 4 || states[1].CommitIndex != 9 || states[1].LeaderAddress != "127.0.0.1:21877" { t.Fatalf("status: %+v %v", states, err) }
	leader.mu.Lock()
	defer leader.mu.Unlock()
	if leader.writes != 1 || leader.attempts < 3 { t.Fatalf("writes=%d attempts=%d", leader.writes, leader.attempts) }
}

func TestRetryRespectsDeadlineAndSequences(t *testing.T) {
	startFake(t, "127.0.0.1:21879", &fakeKV{block: true})
	c, err := New(Config{Endpoints: []string{"127.0.0.1:21879"}, ClientID: "test", RPCTimeout: 40 * time.Millisecond})
	if err != nil { t.Fatal(err) }
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = c.Put(ctx, "key", "value")
	if !errors.Is(err, context.DeadlineExceeded) { t.Fatalf("deadline: %v", err) }
	id, err := c.NewRequest()
	if err != nil || id.Sequence != 2 { t.Fatalf("next identity: %+v %v", id, err) }
	if _, err := c.PutWithID(context.Background(), RequestID{ClientID: "other", Sequence: 1}, "key", "value"); err == nil { t.Fatal("accepted foreign identity") }
}

func TestCloseStopsPendingRetry(t *testing.T) {
	startFake(t, "127.0.0.1:21878", &fakeKV{block: true})
	c, err := New(Config{Endpoints: []string{"127.0.0.1:21878"}, RPCTimeout: time.Second})
	if err != nil { t.Fatal(err) }
	result := make(chan error, 1)
	go func() { _, err := c.Put(context.Background(), "key", "value"); result <- err }()
	time.Sleep(20 * time.Millisecond)
	if err := c.Close(); err != nil { t.Fatal(err) }
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) { t.Fatalf("pending retry: %v", err) }
	case <-time.After(time.Second): t.Fatal("client close did not stop retry")
	}
}
