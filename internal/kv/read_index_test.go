package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/transport"
)

func TestReadLatestQuorumAndNoLogAppend(t *testing.T) {
	c := makeCluster(t, 3, 0)
	leader := c.leader(t, -1)
	ctx, cancel := deadline(t)
	defer cancel()
	if _, err := c.servers[leader].Put(ctx, "writer", 1, "key", "value"); err != nil { t.Fatal(err) }
	before, err := c.servers[leader].Node().Status(ctx)
	if err != nil { t.Fatal(err) }
	got, err := c.servers[leader].ReadLatest(ctx, "key")
	if err != nil || !got.Found || got.Value != "value" || got.Version != 1 { t.Fatalf("read: %+v %v", got, err) }
	after, err := c.servers[leader].Node().Status(ctx)
	if err != nil || after.LogBytes != before.LogBytes { t.Fatalf("read appended to log: before=%+v after=%+v err=%v", before, after, err) }
	c.network.Disconnect(transport.NodeID(leader+1))
	readCtx, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop()
	_, err = c.servers[leader].ReadLatest(readCtx, "key")
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrNotLeader) { t.Fatalf("isolated read: %v", err) }
}

func TestReadIndexNeedsCurrentTermCommit(t *testing.T) {
	c := makeCluster(t, 1, 0)
	leader := c.leader(t, -1)
	ctx, cancel := deadline(t)
	defer cancel()
	_, err := c.servers[leader].ReadLatest(ctx, "key")
	if !errors.Is(err, raft.ErrReadNotReady) { t.Fatalf("read before current-term commit: %v", err) }
	if _, err := c.servers[leader].Put(ctx, "writer", 1, "key", "value"); err != nil { t.Fatal(err) }
	got, err := c.servers[leader].ReadLatest(ctx, "key")
	if err != nil || got.Value != "value" || got.Version != 1 { t.Fatalf("read after commit: %+v %v", got, err) }
}

func TestReadIndexAfterLeaderChange(t *testing.T) {
	c := makeCluster(t, 3, 0)
	leader := c.leader(t, -1)
	ctx, cancel := deadline(t)
	defer cancel()
	if _, err := c.servers[leader].Put(ctx, "writer", 1, "key", "old"); err != nil { t.Fatal(err) }
	c.network.Disconnect(transport.NodeID(leader+1))
	newLeader := c.leader(t, leader)
	_, err := c.servers[newLeader].ReadLatest(ctx, "key")
	if !errors.Is(err, raft.ErrReadNotReady) { t.Fatalf("new leader read without current-term commit: %v", err) }
	if _, err := c.servers[newLeader].Put(ctx, "writer", 2, "key", "new"); err != nil { t.Fatal(err) }
	got, err := c.servers[newLeader].ReadLatest(ctx, "key")
	if err != nil || got.Value != "new" || got.Version != 2 { t.Fatalf("new leader read: %+v %v", got, err) }
}

func TestPorcupineModelDetectsInvalidCAS(t *testing.T) {
	put := command{ClientID: "a", Sequence: 1, Operation: "put", Key: "k", Value: "a"}
	cas := command{ClientID: "b", Sequence: 1, Operation: "cas", Key: "k", Expected: 0, Value: "b"}
	history := []porcupine.Operation{
		{Input: put, Call: 1, Return: 2, Output: historyOutput{Result: Result{Value: "a", Found: true, Version: 1}}},
		{Input: cas, Call: 3, Return: 4, Output: historyOutput{Result: Result{Value: "b", Found: true, Version: 1, Swapped: true}}},
	}
	if porcupine.CheckOperations(kvModel(), history) { t.Fatal("checker accepted stale-version CAS") }
}

func TestLinearizableConcurrentClientsWithPartitionAndCrash(t *testing.T) {
	c := makeCluster(t, 5, 0)
	leader := c.leader(t, -1)
	if err := c.network.Configure(0.1, 50*time.Millisecond); err != nil { t.Fatal(err) }
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	var slots sync.RWMutex
	results := make(chan error, 4)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	begin := make(chan struct{})
	for client := 0; client < 4; client++ {
		client := client
		workers.Add(1)
		go func() {
			defer workers.Done()
			select { case <-begin: case <-ctx.Done(): results <- ctx.Err(); return }
			id, key := fmt.Sprintf("client-%d", client), fmt.Sprintf("key-%d", client%2)
			for sequence := uint64(1); sequence <= 4; sequence++ {
				cmd := command{ClientID: id, Sequence: sequence, Key: key}
				switch sequence {
				case 1: cmd.Operation, cmd.Value = "put", id
				case 2: cmd.Operation = "get"
				case 3: cmd.Operation, cmd.Expected, cmd.Value = "cas", 1, id+"-cas"
				case 4: cmd.Operation = "delete"
				}
				_, err := c.history.invoke(cmd, func() (Result, error) {
					for ctx.Err() == nil {
						slots.RLock()
						servers := append([]*recordedServer(nil), c.servers...)
						slots.RUnlock()
						for _, recorded := range servers {
							if recorded == nil { continue }
							attempt, stop := context.WithTimeout(ctx, 200*time.Millisecond)
							var value Result
							var err error
							s := recorded.Server
							switch cmd.Operation {
							case "put": value, err = s.Put(attempt, id, sequence, key, cmd.Value)
							case "get": value, err = s.Get(attempt, id, sequence, key)
							case "cas": value, err = s.CompareAndSwap(attempt, id, sequence, key, cmd.Expected, cmd.Value)
							case "delete": value, err = s.Delete(attempt, id, sequence, key)
							}
							stop()
							if err == nil { return value, nil }
							if !errors.Is(err, ErrNotLeader) && !errors.Is(err, raft.ErrStopped) && !errors.Is(err, ErrStopped) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) { return Result{}, err }
						}
						time.Sleep(10 * time.Millisecond)
					}
					return Result{}, ctx.Err()
				})
				if err != nil { results <- err; return }
			}
			results <- nil
		}()
	}
	companion := (leader + 1) % 5
	minority, majority := []transport.NodeID{}, []transport.NodeID{}
	for i := 0; i < 5; i++ {
		if i == leader || i == companion { minority = append(minority, transport.NodeID(i+1)) } else { majority = append(majority, transport.NodeID(i+1)) }
	}
	if err := c.network.Partition(minority, majority); err != nil { t.Fatal(err) }
	close(begin)
	time.Sleep(25 * time.Millisecond)
	c.network.Disconnect(transport.NodeID(leader+1))
	slots.Lock()
	old := c.servers[leader]
	c.servers[leader] = nil
	slots.Unlock()
	old.Close()
	time.Sleep(150 * time.Millisecond)
	peers := make([]transport.NodeID, 0, 4)
	for i := 0; i < 5; i++ { if i != leader { peers = append(peers, transport.NodeID(i+1)) } }
	id := transport.NodeID(leader+1)
	restarted, err := New(raft.Config{ID: id, Peers: peers, Transport: c.network.Endpoint(id), Storage: c.stores[leader]}, 0)
	if err != nil { t.Fatal(err) }
	c.network.Register(id, restarted.Node())
	restarted.Start()
	slots.Lock()
	c.servers[leader] = &recordedServer{Server: restarted, history: c.history}
	slots.Unlock()
	time.Sleep(300 * time.Millisecond)
	if err := c.network.Partition(); err != nil { t.Fatal(err) }
	workers.Wait()
	for i := 0; i < 4; i++ { if err := <-results; err != nil { t.Fatal(err) } }
}

func BenchmarkReads(b *testing.B) {
	for _, fast := range []bool{false, true} {
		name := "logged"
		if fast { name = "read-index" }
		b.Run(name, func(b *testing.B) {
			network := transport.NewNetwork(1)
			s, err := New(raft.Config{ID: 1, Transport: network.Endpoint(1), Storage: &raft.MemoryStorage{}}, 1<<20)
			if err != nil { b.Fatal(err) }
			network.Register(1, s.Node())
			s.Start()
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for {
				status, err := s.Node().Status(ctx)
				if err != nil { b.Fatal(err) }
				if status.Role == raft.Leader { break }
				time.Sleep(time.Millisecond)
			}
			if _, err := s.Put(ctx, "init", 1, "key", "value"); err != nil { b.Fatal(err) }
			var sequence atomic.Uint64
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				var err error
				if fast { _, err = s.ReadLatest(context.Background(), "key") } else { _, err = s.Get(context.Background(), "bench", sequence.Add(1), "key") }
				if err != nil { b.Fatal(err) }
			}
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "reads/s")
		})
	}
}
