package raft

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/you/raftkv/internal/transport"
)

func TestDiskClusterRestarts(t *testing.T) {
	seed := int64(314159)
	if value := os.Getenv("RAFTKV_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil { t.Fatal(err) }
	}
	random := rand.New(rand.NewSource(seed))
	const count = 3
	dirs := make([]string, count)
	stores := make([]*DiskStorage, count)
	for i := range dirs { dirs[i] = t.TempDir() }
	t.Cleanup(func() {
		for _, store := range stores { if store != nil { _ = store.Close() } }
	})
	makeNode := func(id transport.NodeID, tr transport.Transport) clusterNode {
		i := int(id) - 1
		var err error
		stores[i], err = OpenDiskStorage(dirs[i])
		if err != nil { t.Fatal(err) }
		peers := make([]transport.NodeID, 0, count-1)
		for p := 1; p <= count; p++ { if transport.NodeID(p) != id { peers = append(peers, transport.NodeID(p)) } }
		applied := make(chan ApplyMsg, 1024)
		node, err := NewNode(Config{ID: id, Peers: peers, Transport: tr, Storage: stores[i], Seed: seed + int64(id), ApplyCh: applied})
		if err != nil { t.Fatal(err) }
		return &electionNode{Node: node, t: t, applied: applied}
	}
	c := newTestCluster(t, count, makeNode)
	stop := func(i int) {
		c.disconnect(i)
		c.nodes[i].stop()
		if err := stores[i].Close(); err != nil { t.Fatal(err) }
	}
	start := func(i int) {
		id := transport.NodeID(i + 1)
		c.nodes[i] = makeNode(id, c.network.Endpoint(id))
		c.network.Register(id, c.nodes[i])
		c.active[i] = true
		c.nodes[i].start()
	}
	committed := make(map[uint64]string)
	observeCommitted := func() {
		t.Helper()
		for i, node := range c.nodes {
			if !c.active[i] { continue }
			_, _, index := node.status()
			entries, err := stores[i].LoadLog()
			if err != nil || uint64(len(entries)) < index { t.Fatalf("node %d committed log unavailable: %v", i, err) }
			for j := uint64(0); j < index; j++ {
				command := string(entries[j].Command)
				if prior, ok := committed[j+1]; ok && prior != command { t.Fatalf("conflicting committed entry at %d", j+1) }
				committed[j+1] = command
			}
		}
	}
	checkCommitted := func(index uint64) {
		t.Helper()
		for i, store := range stores {
			entries, err := store.LoadLog()
			if err != nil { t.Fatal(err) }
			if uint64(len(entries)) < index { t.Fatalf("node %d lost committed index %d", i, index) }
			for position, command := range committed {
				if uint64(len(entries)) < position || string(entries[position-1].Command) != command { t.Fatalf("node %d lost or changed committed index %d", i, position) }
			}
		}
	}
	for round := 0; round < 6; round++ {
		leader := c.checkOneLeader()
		command := fmt.Sprintf("committed-%d", round)
		index := submitCommand(t, c, leader, command)
		c.waitCommitted(index, count)
		committed[index] = command
		checkCommitted(index)
		// Start another write and stop nodes at a seeded point without waiting
		// for it to commit; it may survive or be overwritten after restart.
		submitCommand(t, c, leader, fmt.Sprintf("inflight-%d", round))
		observeCommitted()
		if round == 2 || round == 5 {
			order := random.Perm(count)
			for _, i := range order { observeCommitted(); stop(i) }
			for _, i := range order { start(i) }
		} else {
			i := random.Intn(count)
			observeCommitted()
			stop(i)
			start(i)
		}
		leader = c.checkOneLeader()
		barrier := submitCommand(t, c, leader, fmt.Sprintf("barrier-%d", round))
		c.waitCommitted(barrier, count)
		checkCommitted(index)
		var reference []transport.LogEntry
		for i, store := range stores {
			entries, err := store.LoadLog()
			if err != nil { t.Fatal(err) }
			if uint64(len(entries)) < barrier { t.Fatalf("node %d missing barrier", i) }
			if i == 0 { reference = entries[:barrier]; continue }
			for j := range reference {
				if reference[j].Term != entries[j].Term || string(reference[j].Command) != string(entries[j].Command) { t.Fatalf("node %d differs at index %d", i, j+1) }
			}
		}
	}
}
