package kv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/transport"
)

type testCluster struct {
	network *transport.Network
	servers []*Server
	stores []*raft.MemoryStorage
}

func makeCluster(t *testing.T, count int, threshold uint64) *testCluster {
	t.Helper()
	seed := int64(314159)
	if value := os.Getenv("RAFTKV_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil { t.Fatal(err) }
	}
	c := &testCluster{network: transport.NewNetwork(seed), servers: make([]*Server, count), stores: make([]*raft.MemoryStorage, count)}
	t.Cleanup(func() { for _, s := range c.servers { if s != nil { s.Close() } } })
	t.Cleanup(func() { if t.Failed() { t.Logf("replay with RAFTKV_TEST_SEED=%d", seed) } })
	for i := range c.servers {
		id := transport.NodeID(i+1)
		peers := make([]transport.NodeID, 0, count-1)
		for j := range c.servers { if j != i { peers = append(peers, transport.NodeID(j+1)) } }
		c.stores[i] = &raft.MemoryStorage{}
		server, err := New(raft.Config{ID: id, Peers: peers, Transport: c.network.Endpoint(id), Storage: c.stores[i], Seed: seed + int64(id)}, threshold)
		if err != nil { t.Fatal(err) }
		c.servers[i] = server
		c.network.Register(id, server.Node())
	}
	for _, server := range c.servers { server.Start() }
	return c
}

func (c *testCluster) leader(t *testing.T, exclude int) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, server := range c.servers {
			if i == exclude { continue }
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			status, err := server.Node().Status(ctx)
			cancel()
			if err == nil && status.Role == raft.Leader { return i }
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cluster did not elect a leader")
	return -1
}

func deadline(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func TestDuplicateAfterLeaderChange(t *testing.T) {
	c := makeCluster(t, 3, 0)
	leader := c.leader(t, -1)
	ctx, cancel := deadline(t)
	defer cancel()
	first, err := c.servers[leader].Put(ctx, "client", 1, "key", "value")
	if err != nil || first.Version != 1 || !first.Found { t.Fatalf("put: %+v %v", first, err) }
	c.network.Disconnect(transport.NodeID(leader+1))
	newLeader := c.leader(t, leader)
	retry, err := c.servers[newLeader].Put(ctx, "client", 1, "key", "value")
	if err != nil || retry != first { t.Fatalf("retry: %+v %v, want %+v", retry, err, first) }
	got, err := c.servers[newLeader].Get(ctx, "client", 2, "key")
	if err != nil || got.Version != 1 || got.Value != "value" { t.Fatalf("get after retry: %+v %v", got, err) }
	_, err = c.servers[newLeader].Put(ctx, "client", 1, "key", "different")
	if !errors.Is(err, ErrRequestConflict) { t.Fatalf("sequence reuse: %v", err) }
}

func TestConcurrentCompareAndSwap(t *testing.T) {
	c := makeCluster(t, 3, 0)
	leader := c.leader(t, -1)
	ctx, cancel := deadline(t)
	defer cancel()
	initial, err := c.servers[leader].Put(ctx, "init", 1, "key", "initial")
	if err != nil || initial.Version != 1 { t.Fatalf("initial put: %+v %v", initial, err) }
	type outcome struct { result Result; err error }
	results := make(chan outcome, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			ready.Done()
			<-start
			result, err := c.servers[leader].CompareAndSwap(ctx, fmt.Sprintf("client-%d", i), 1, "key", 1, fmt.Sprintf("value-%d", i))
			results <- outcome{result, err}
		}()
	}
	ready.Wait()
	close(start)
	winners := 0
	for i := 0; i < 2; i++ {
		out := <-results
		if out.err != nil { t.Fatal(out.err) }
		if out.result.Swapped { winners++ }
		if out.result.Version != 2 { t.Fatalf("CAS version: %+v", out.result) }
	}
	if winners != 1 { t.Fatalf("CAS winners = %d", winners) }
	got, err := c.servers[leader].Get(ctx, "reader", 1, "key")
	if err != nil || got.Version != 2 || (got.Value != "value-0" && got.Value != "value-1") { t.Fatalf("final value: %+v %v", got, err) }
}

func TestVersionsAndReadRetries(t *testing.T) {
	c := makeCluster(t, 1, 0)
	leader := c.leader(t, -1)
	s := c.servers[leader]
	ctx, cancel := deadline(t)
	defer cancel()
	missing, err := s.Get(ctx, "reader", 1, "key")
	if err != nil || missing.Found || missing.Version != 0 { t.Fatalf("missing: %+v %v", missing, err) }
	put, err := s.Put(ctx, "writer", 1, "key", "a")
	if err != nil || put.Version != 1 { t.Fatalf("put: %+v %v", put, err) }
	retry, err := s.Get(ctx, "reader", 1, "key")
	if err != nil || retry != missing { t.Fatalf("read retry: %+v %v", retry, err) }
	deleted, err := s.Delete(ctx, "writer", 2, "key")
	if err != nil || deleted.Found || deleted.Version != 2 { t.Fatalf("delete: %+v %v", deleted, err) }
	cas, err := s.CompareAndSwap(ctx, "writer", 3, "key", 0, "b")
	if err != nil || cas.Swapped || cas.Version != 2 { t.Fatalf("stale CAS: %+v %v", cas, err) }
	cas, err = s.CompareAndSwap(ctx, "writer", 4, "key", 2, "b")
	if err != nil || !cas.Swapped || !cas.Found || cas.Version != 3 { t.Fatalf("recreate CAS: %+v %v", cas, err) }
	if _, err := s.Put(ctx, "writer", 5, "key", "c"); err != nil { t.Fatal(err) }
	retriedCAS, err := s.CompareAndSwap(ctx, "writer", 4, "key", 2, "b")
	if err != nil || retriedCAS != cas { t.Fatalf("CAS retry: %+v %v, want %+v", retriedCAS, err, cas) }
}

func TestSnapshotRestoresValuesAndDeduplication(t *testing.T) {
	c := makeCluster(t, 1, 1)
	leader := c.leader(t, -1)
	ctx, cancel := deadline(t)
	defer cancel()
	first, err := c.servers[leader].Put(ctx, "writer", 1, "key", "value")
	if err != nil { t.Fatal(err) }
	end := time.Now().Add(3 * time.Second)
	for {
		snapshot, err := c.stores[leader].LoadSnapshot()
		if err != nil { t.Fatal(err) }
		if snapshot.Index >= 1 { break }
		if time.Now().After(end) { t.Fatal("snapshot not created") }
		time.Sleep(time.Millisecond)
	}
	c.servers[leader].Close()
	id := transport.NodeID(leader + 1)
	restarted, err := New(raft.Config{ID: id, Transport: c.network.Endpoint(id), Storage: c.stores[leader]}, 1)
	if err != nil { t.Fatal(err) }
	c.servers[leader] = restarted
	c.network.Register(id, restarted.Node())
	restarted.Start()
	c.leader(t, -1)
	retry, err := restarted.Put(ctx, "writer", 1, "key", "value")
	if err != nil || retry != first { t.Fatalf("retry after snapshot: %+v %v", retry, err) }
	got, err := restarted.Get(ctx, "reader", 1, "key")
	if err != nil || !got.Found || got.Value != "value" || got.Version != 1 { t.Fatalf("restored value: %+v %v", got, err) }
}

func TestIsolatedLeaderCannotServeNewRead(t *testing.T) {
	c := makeCluster(t, 3, 0)
	leader := c.leader(t, -1)
	ctx, cancel := deadline(t)
	defer cancel()
	if _, err := c.servers[leader].Put(ctx, "writer", 1, "key", "value"); err != nil { t.Fatal(err) }
	c.network.Disconnect(transport.NodeID(leader + 1))
	readCtx, stopRead := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stopRead()
	_, err := c.servers[leader].Get(readCtx, "reader", 1, "key")
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrNotLeader) { t.Fatalf("isolated read returned %v", err) }
}
