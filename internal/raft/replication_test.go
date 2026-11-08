package raft

import (
	"context"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

func submitCommand(t *testing.T, c *testCluster, leader int, command string) uint64 {
	t.Helper()
	index, _, ok := c.nodes[leader].(*electionNode).Start([]byte(command))
	if !ok { t.Fatal("node rejected command as non-leader") }
	return index
}

func expectApplied(t *testing.T, c *testCluster, node int, index uint64, command string) {
	t.Helper()
	select {
	case msg := <-c.nodes[node].(*electionNode).applied:
		if msg.Index != index || string(msg.Command) != command { t.Fatalf("node %d applied %+v, want %d %q", node, msg, index, command) }
	case <-time.After(3 * time.Second):
		t.Fatalf("node %d did not apply index %d", node, index)
	}
}

func TestReplicationAndFollowerCatchup(t *testing.T) {
	c := electionCluster(t, 3)
	leader := c.checkOneLeader()
	index := submitCommand(t, c, leader, "one")
	c.waitCommitted(index, 3)
	for i := range c.nodes { expectApplied(t, c, i, index, "one") }
	follower := (leader + 1) % 3
	c.disconnect(follower)
	index = submitCommand(t, c, leader, "two")
	c.waitCommitted(index, 2)
	index2 := submitCommand(t, c, leader, "three")
	c.waitCommitted(index2, 2)
	c.reconnect(follower)
	c.waitCommitted(index2, 3)
	for i := range c.nodes {
		expectApplied(t, c, i, index, "two")
		expectApplied(t, c, i, index2, "three")
	}
}

func TestMinorityCannotCommitAndOldLeaderLogIsOverwritten(t *testing.T) {
	c := electionCluster(t, 3)
	old := c.checkOneLeader()
	first := submitCommand(t, c, old, "committed")
	c.waitCommitted(first, 3)
	for i := range c.nodes { expectApplied(t, c, i, first, "committed") }
	c.disconnect(old)
	stale := submitCommand(t, c, old, "uncommitted")
	time.Sleep(350 * time.Millisecond)
	_, _, commit := c.nodes[old].status()
	if commit >= stale { t.Fatal("minority committed an entry") }
	newLeader := c.checkOneLeader()
	replacement := submitCommand(t, c, newLeader, "replacement")
	if replacement != stale { t.Fatalf("replacement index %d, old index %d", replacement, stale) }
	c.waitCommitted(replacement, 2)
	c.reconnect(old)
	c.waitCommitted(replacement, 3)
	for i := range c.nodes { expectApplied(t, c, i, replacement, "replacement") }
}

func TestFigureEightCommitRule(t *testing.T) {
	n := &Node{config: Config{Peers: []transport.NodeID{2, 3, 4, 5}}}
	s := &nodeState{node: n, hard: HardState{Term: 4}, log: []transport.LogEntry{{Term: 2}}, matchIndex: map[transport.NodeID]uint64{2: 1, 3: 1}}
	s.advanceCommit()
	if s.commitIndex != 0 { t.Fatal("committed prior-term entry by counting replicas") }
	s.log = append(s.log, transport.LogEntry{Term: 4})
	s.matchIndex[2], s.matchIndex[3] = 2, 2
	s.advanceCommit()
	if s.commitIndex != 2 { t.Fatalf("commit index = %d, want 2", s.commitIndex) }
}

func TestFollowerConsistencyAndPersistence(t *testing.T) {
	network := transport.NewNetwork(1)
	store := &MemoryStorage{}
	if err := store.SaveLog([]transport.LogEntry{{Term: 1, Command: []byte("a")}, {Term: 2, Command: []byte("old")}}); err != nil { t.Fatal(err) }
	node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: store, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	ctx := context.Background()
	bad, err := node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: 3, PrevLogIndex: 1, PrevLogTerm: 9})
	if err != nil || bad.Success { t.Fatalf("accepted inconsistent prefix: %+v %v", bad, err) }
	good, err := node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: 3, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []transport.LogEntry{{Term: 3, Command: []byte("new")}}})
	if err != nil || !good.Success { t.Fatalf("append failed: %+v %v", good, err) }
	entries, err := store.LoadLog()
	if err != nil || len(entries) != 2 || entries[1].Term != 3 || string(entries[1].Command) != "new" { t.Fatalf("persisted log: %+v %v", entries, err) }
}

func TestBlockedApplyDoesNotBlockNode(t *testing.T) {
	network := transport.NewNetwork(1)
	applied := make(chan ApplyMsg)
	node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: &MemoryStorage{}, ApplyCh: applied, ElectionMin: 30 * time.Millisecond, ElectionMax: 30 * time.Millisecond, Heartbeat: 10 * time.Millisecond})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, err := node.Status(context.Background())
		if err != nil { t.Fatal(err) }
		if status.Role == Leader { break }
		if time.Now().After(deadline) { t.Fatal("single node did not elect itself") }
		time.Sleep(time.Millisecond)
	}
	index, _, ok := node.Start([]byte("one"))
	if !ok || index != 1 { t.Fatalf("start: %d %v", index, ok) }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := node.Status(ctx)
	if err != nil || status.CommitIndex != 1 { t.Fatalf("blocked apply prevented status: %+v %v", status, err) }
	select {
	case msg := <-applied:
		if msg.Index != 1 || string(msg.Command) != "one" { t.Fatalf("unexpected apply: %+v", msg) }
	case <-ctx.Done():
		t.Fatal("committed command not delivered")
	}
}
