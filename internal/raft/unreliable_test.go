package raft

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

type observedEntry struct { term uint64; command string }

type appliedHistory struct {
	mu sync.Mutex
	entries map[uint64]observedEntry
	nodes []map[uint64]observedEntry
	err string
}

func (h *appliedHistory) record(node int, msg ApplyMsg, expected uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := observedEntry{msg.Term, string(msg.Command)}
	if msg.Index != expected && h.err == "" { h.err = fmt.Sprintf("node %d applied %d after %d", node, msg.Index, expected-1) }
	if prev, ok := h.entries[msg.Index]; ok && prev != entry && h.err == "" { h.err = fmt.Sprintf("conflicting applies at index %d: %+v and %+v", msg.Index, prev, entry) }
	h.entries[msg.Index] = entry
	h.nodes[node][msg.Index] = entry
}

// TestUnreliablePartitionsAndLeaderCrashes retains loss, delay and reordering
// through recovery. Node crashes are modeled by stopping and recreating nodes.
func TestUnreliablePartitionsAndLeaderCrashes(t *testing.T) {
	seed := int64(314159)
	if value := os.Getenv("RAFTKV_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil { t.Fatal(err) }
	}
	t.Cleanup(func() { if t.Failed() { t.Logf("replay with RAFTKV_TEST_SEED=%d (goroutine scheduling also affects delivery)", seed) } })
	random := rand.New(rand.NewSource(seed))
	const count = 5
	network := transport.NewNetwork(seed)
	if err := network.Configure(0.1, 50*time.Millisecond); err != nil { t.Fatal(err) }
	stores := make([]*MemoryStorage, count)
	nodes := make([]*Node, count)
	var nodesMu sync.RWMutex
	history := &appliedHistory{entries: make(map[uint64]observedEntry), nodes: make([]map[uint64]observedEntry, count)}
	for i := range history.nodes { history.nodes[i] = make(map[uint64]observedEntry); stores[i] = &MemoryStorage{} }
	applyChannels := make([]chan ApplyMsg, count)
	var collectors sync.WaitGroup
	start := func(i int) {
		id := transport.NodeID(i + 1)
		peers := make([]transport.NodeID, 0, count-1)
		for j := 0; j < count; j++ { if j != i { peers = append(peers, transport.NodeID(j+1)) } }
		applied := make(chan ApplyMsg, 1024)
		applyChannels[i] = applied
		node, err := NewNode(Config{ID: id, Peers: peers, Storage: stores[i], Transport: network.Endpoint(id), ApplyCh: applied, Seed: seed + int64(id)})
		if err != nil { t.Fatal(err) }
		collectors.Add(1)
		go func() {
			defer collectors.Done()
			expected := uint64(1)
			for msg := range applied {
				history.record(i, msg, expected)
				expected = msg.Index + 1
			}
		}()
		network.Register(id, node)
		node.Run()
		nodesMu.Lock()
		nodes[i] = node
		nodesMu.Unlock()
	}
	snapshot := func() []*Node {
		nodesMu.RLock()
		defer nodesMu.RUnlock()
		return append([]*Node(nil), nodes...)
	}
	for i := 0; i < count; i++ { start(i) }
	clientCtx, cancelClient := context.WithCancel(context.Background())
	clientDone := make(chan struct{})
	var proposals atomic.Int64
	go func() {
		defer close(clientDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for sequence := 0; ; sequence++ {
			select { case <-clientCtx.Done(): return; case <-ticker.C: }
			for _, node := range snapshot() {
				if node == nil { continue }
				ctx, cancel := context.WithTimeout(clientCtx, 100*time.Millisecond)
				_, _, ok, err := node.Propose(ctx, []byte(fmt.Sprintf("client-%d", sequence)))
				cancel()
				if err == nil && ok { proposals.Add(1); break }
			}
		}
	}()
	t.Cleanup(func() {
		cancelClient()
		<-clientDone
		for i, node := range snapshot() {
			if node != nil { node.Stop(); close(applyChannels[i]) }
		}
		collectors.Wait()
		history.mu.Lock()
		defer history.mu.Unlock()
		if history.err != "" { t.Error(history.err) }
	})
	findLeader := func() int {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for i, node := range snapshot() {
				if node == nil { continue }
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				status, err := node.Status(ctx)
				cancel()
				if err == nil && status.Role == Leader { return i }
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("no leader found")
		return -1
	}
	for round := 0; round < 3; round++ {
		leader := findLeader()
		order := random.Perm(count)
		groups := [][]transport.NodeID{{}, {}}
		for i, node := range order {
			group := 0
			if i >= 2 { group = 1 }
			groups[group] = append(groups[group], transport.NodeID(node+1))
		}
		if err := network.Partition(groups...); err != nil { t.Fatal(err) }
		time.Sleep(200 * time.Millisecond)
		network.Disconnect(transport.NodeID(leader + 1))
		nodesMu.Lock()
		old := nodes[leader]
		nodes[leader] = nil
		nodesMu.Unlock()
		old.Stop()
		close(applyChannels[leader])
		time.Sleep(150 * time.Millisecond)
		start(leader)
		time.Sleep(250 * time.Millisecond)
		if err := network.Partition(); err != nil { t.Fatal(err) }
		time.Sleep(200 * time.Millisecond)
	}
	cancelClient()
	<-clientDone
	if proposals.Load() == 0 { t.Fatal("client made no accepted proposals during faults") }
	// Retry a barrier without disabling loss; a successful proposal can still be
	// lost before commitment, so multiple barriers may appear in the log.
	deadline := time.Now().Add(10 * time.Second)
	lastProposal := time.Time{}
	stableSince := time.Time{}
	stableCount := 0
	for time.Now().Before(deadline) {
		history.mu.Lock()
		failure := history.err
		complete := false
		clientApplied := false
		allApplied := true
		for index, entry := range history.entries {
			if len(entry.command) >= 7 && entry.command[:7] == "client-" { clientApplied = true }
			all := true
			for _, observed := range history.nodes { if observed[index] != entry { all = false; break } }
			if !all { allApplied = false }
			if entry.command == "barrier" && all { complete = true }
		}
		countApplied := len(history.entries)
		history.mu.Unlock()
		if failure != "" { t.Fatal(failure) }
		if complete && clientApplied && allApplied {
			if stableSince.IsZero() || countApplied != stableCount { stableSince, stableCount = time.Now(), countApplied }
			if time.Since(stableSince) >= 300*time.Millisecond { return }
		} else { stableSince = time.Time{} }
		if stableSince.IsZero() && time.Since(lastProposal) >= 250*time.Millisecond {
			for _, node := range snapshot() {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_, _, ok, err := node.Propose(ctx, []byte("barrier"))
				cancel()
				if err == nil && ok { break }
			}
			lastProposal = time.Now()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("cluster failed to converge and apply client commands and a common barrier under loss")
}
