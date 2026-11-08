package raft

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

// clusterNode is the adapter real Raft nodes will implement in future tests.
type clusterNode interface {
	transport.Handler
	start()
	stop()
	status() (term uint64, leader bool, commitIndex uint64)
}

type testCluster struct {
	t       *testing.T
	network *transport.Network
	nodes   []clusterNode
	active  []bool
}

func newTestCluster(t *testing.T, count int, factory func(transport.NodeID, transport.Transport) clusterNode) *testCluster {
	t.Helper()
	seed := int64(314159)
	if value := os.Getenv("RAFTKV_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil { t.Fatalf("invalid RAFTKV_TEST_SEED %q: %v", value, err) }
	}
	c := &testCluster{t: t, network: transport.NewNetwork(seed), nodes: make([]clusterNode, count), active: make([]bool, count)}
	t.Cleanup(func() {
		for _, node := range c.nodes {
			if node != nil { node.stop() }
		}
		if t.Failed() { t.Logf("replay with RAFTKV_TEST_SEED=%d", seed) }
	})
	for i := range c.nodes {
		id := transport.NodeID(i + 1)
		c.nodes[i] = factory(id, c.network.Endpoint(id))
		c.network.Register(id, c.nodes[i])
		c.active[i] = true
	}
	for _, node := range c.nodes { node.start() }
	return c
}

func (c *testCluster) disconnect(i int) {
	c.t.Helper()
	c.network.Disconnect(transport.NodeID(i + 1))
	c.active[i] = false
}

func (c *testCluster) reconnect(i int) {
	c.t.Helper()
	c.network.Reconnect(transport.NodeID(i + 1))
	c.active[i] = true
}

func (c *testCluster) checkOneLeader() int {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leader, count := -1, 0
		for i, node := range c.nodes {
			_, isLeader, _ := node.status()
			if c.active[i] && isLeader { leader, count = i, count+1 }
		}
		if count == 1 { return leader }
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("expected one connected leader")
	return -1
}

func (c *testCluster) checkNoLeader() {
	c.t.Helper()
	for i, node := range c.nodes {
		_, leader, _ := node.status()
		if c.active[i] && leader { c.t.Fatalf("node %d is leader", i+1) }
	}
}

func (c *testCluster) checkTerms() uint64 {
	c.t.Helper()
	var term uint64
	found := false
	for i, node := range c.nodes {
		if !c.active[i] { continue }
		current, _, _ := node.status()
		if found && term != current { c.t.Fatalf("terms differ: %d and %d", term, current) }
		term, found = current, true
	}
	if !found { c.t.Fatal("no connected nodes") }
	return term
}

func (c *testCluster) waitCommitted(index uint64, n int) {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, node := range c.nodes {
			_, _, committed := node.status()
			if committed >= index { count++ }
		}
		if count >= n { return }
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("index %d not committed on %d nodes", index, n)
}
