package transport

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

func testSeed(t *testing.T) int64 {
	t.Helper()
	seed := int64(314159)
	if value := os.Getenv("RAFTKV_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("invalid RAFTKV_TEST_SEED %q: %v", value, err)
		}
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("replay with RAFTKV_TEST_SEED=%d", seed)
		}
	})
	return seed
}

type testHandler struct{}

func (testHandler) RequestVote(_ context.Context, r RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{Term: r.Term, VoteGranted: true}, nil
}

func (testHandler) AppendEntries(_ context.Context, r AppendEntriesRequest) (AppendEntriesResponse, error) {
	if len(r.Entries) > 0 && len(r.Entries[0].Command) > 0 {
		r.Entries[0].Command[0] = 'x'
	}
	return AppendEntriesResponse{Term: r.Term, Success: true}, nil
}

func (testHandler) InstallSnapshot(_ context.Context, r InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	if len(r.Data) > 0 {
		r.Data[0] = 'x'
	}
	return InstallSnapshotResponse{Term: r.Term}, nil
}

func TestNetworkTopologyAndRPCs(t *testing.T) {
	n := NewNetwork(testSeed(t))
	for id := NodeID(1); id <= 3; id++ {
		n.Register(id, testHandler{})
	}
	ctx := context.Background()
	a := n.Endpoint(1)
	vote := func(to NodeID) error {
		response, err := a.RequestVote(ctx, to, RequestVoteRequest{Term: 7})
		if err == nil && (!response.VoteGranted || response.Term != 7) {
			t.Errorf("unexpected vote: %+v", response)
		}
		return err
	}
	if err := vote(2); err != nil { t.Fatal(err) }
	n.Disconnect(2)
	if err := vote(2); !errors.Is(err, ErrUnavailable) { t.Fatalf("disconnected: %v", err) }
	n.Reconnect(2)
	if err := n.Partition([]NodeID{1, 2}, []NodeID{3}); err != nil { t.Fatal(err) }
	if err := vote(2); err != nil { t.Fatal(err) }
	if err := vote(3); !errors.Is(err, ErrUnavailable) { t.Fatalf("partitioned: %v", err) }
	if err := n.Partition([]NodeID{1}, []NodeID{1}); err == nil { t.Fatal("accepted duplicate node") }
	if err := n.Partition(); err != nil { t.Fatal(err) }
	if err := vote(3); err != nil { t.Fatal(err) }
	command := []byte("abc")
	response, err := a.AppendEntries(ctx, 2, AppendEntriesRequest{Term: 8, Entries: []LogEntry{{Command: command}}})
	if err != nil || !response.Success || response.Term != 8 || string(command) != "abc" { t.Fatalf("append: %+v %v %q", response, err, command) }
	data := []byte("abc")
	snapshot, err := a.InstallSnapshot(ctx, 2, InstallSnapshotRequest{Term: 9, Data: data})
	if err != nil || snapshot.Term != 9 || string(data) != "abc" { t.Fatalf("snapshot: %+v %v %q", snapshot, err, data) }
}

func TestNetworkLossAndCancellation(t *testing.T) {
	n := NewNetwork(testSeed(t))
	n.Register(1, testHandler{})
	n.Register(2, testHandler{})
	if err := n.Configure(1, 0); err != nil { t.Fatal(err) }
	if _, err := n.Endpoint(1).RequestVote(context.Background(), 2, RequestVoteRequest{}); !errors.Is(err, ErrUnavailable) { t.Fatalf("drop: %v", err) }
	if err := n.Configure(-1, 0); err == nil { t.Fatal("accepted invalid loss") }
	if err := n.Configure(0, time.Second); err != nil { t.Fatal(err) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := n.Endpoint(1).RequestVote(ctx, 2, RequestVoteRequest{}); !errors.Is(err, context.Canceled) { t.Fatalf("cancellation: %v", err) }
}

func TestSeedReplaysLoss(t *testing.T) {
	seed := testSeed(t)
	sequence := func() []bool {
		n := NewNetwork(seed)
		n.Register(1, testHandler{})
		n.Register(2, testHandler{})
		if err := n.Configure(0.3, 0); err != nil { t.Fatal(err) }
		result := make([]bool, 100)
		for i := range result {
			_, err := n.Endpoint(1).RequestVote(context.Background(), 2, RequestVoteRequest{})
			result[i] = err == nil
		}
		return result
	}
	a, b := sequence(), sequence()
	for i := range a {
		if a[i] != b[i] { t.Fatalf("sequence differs at %d", i) }
	}
}

type blockingHandler struct {
	testHandler
	entered chan struct{}
	release chan struct{}
}

func (h blockingHandler) RequestVote(ctx context.Context, request RequestVoteRequest) (RequestVoteResponse, error) {
	if request.Term == 1 {
		close(h.entered)
		select {
		case <-h.release:
		case <-ctx.Done():
			return RequestVoteResponse{}, ctx.Err()
		}
	}
	return RequestVoteResponse{Term: request.Term, VoteGranted: true}, nil
}

func TestConcurrentDeliveryAndInFlightPartition(t *testing.T) {
	n := NewNetwork(testSeed(t))
	h := blockingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	n.Register(1, testHandler{})
	n.Register(2, h)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := n.Endpoint(1).RequestVote(ctx, 2, RequestVoteRequest{Term: 1})
		first <- err
	}()
	select {
	case <-h.entered:
	case <-ctx.Done():
		t.Fatal("first request did not arrive")
	}
	response, err := n.Endpoint(1).RequestVote(ctx, 2, RequestVoteRequest{Term: 2})
	if err != nil || response.Term != 2 { t.Fatalf("second request: %+v %v", response, err) }
	if err := n.Partition([]NodeID{1}, []NodeID{2}); err != nil { t.Fatal(err) }
	close(h.release)
	select {
	case err := <-first:
		if !errors.Is(err, ErrUnavailable) { t.Fatalf("in-flight response: %v", err) }
	case <-ctx.Done():
		t.Fatal("first request did not finish")
	}
}

type responseDropHandler struct {
	testHandler
	network *Network
}

func (h responseDropHandler) RequestVote(_ context.Context, request RequestVoteRequest) (RequestVoteResponse, error) {
	if err := h.network.Configure(1, 0); err != nil {
		return RequestVoteResponse{}, err
	}
	return RequestVoteResponse{Term: request.Term, VoteGranted: true}, nil
}

func TestResponseCanBeLostAfterHandling(t *testing.T) {
	n := NewNetwork(testSeed(t))
	n.Register(1, testHandler{})
	n.Register(2, responseDropHandler{network: n})
	_, err := n.Endpoint(1).RequestVote(context.Background(), 2, RequestVoteRequest{Term: 3})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("response should be dropped, got %v", err)
	}
}
