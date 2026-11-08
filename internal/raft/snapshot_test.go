package raft

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

func receiveApply(t *testing.T, ch <-chan ApplyMsg) ApplyMsg {
	t.Helper()
	select {
	case msg := <-ch: return msg
	case <-time.After(3 * time.Second): t.Fatal("timed out waiting for apply"); return ApplyMsg{}
	}
}

func TestSnapshotBoundsLogAndCatchesUpFollower(t *testing.T) {
	c := newTestCluster(t, 3, func(id transport.NodeID, tr transport.Transport) clusterNode {
		peers := make([]transport.NodeID, 0, 2)
		for i := transport.NodeID(1); i <= 3; i++ { if i != id { peers = append(peers, i) } }
		applied := make(chan ApplyMsg, 128)
		node, err := NewNode(Config{ID: id, Peers: peers, Transport: tr, Storage: &MemoryStorage{}, ApplyCh: applied, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: 10 * time.Millisecond})
		if err != nil { t.Fatal(err) }
		return &electionNode{Node: node, t: t, applied: applied}
	})
	_, err := call(context.Background(), c.nodes[0].(*electionNode).Node, func(s *nodeState) (struct{}, error) { s.elect(); return struct{}{}, s.err })
	if err != nil { t.Fatal(err) }
	leader := c.checkOneLeader()
	follower := (leader + 1) % 3
	c.disconnect(follower)
	node := c.nodes[leader].(*electionNode)
	var state []string
	for i := 1; i <= 30; i++ {
		command := fmt.Sprintf("value-%d", i)
		index := submitCommand(t, c, leader, command)
		c.waitCommitted(index, 2)
		msg := receiveApply(t, node.applied)
		if msg.SnapshotValid || msg.Index != index || string(msg.Command) != command { t.Fatalf("unexpected apply: %+v", msg) }
		state = append(state, command)
		if i%5 == 0 {
			if err := node.Snapshot(index, []byte(strings.Join(state, ","))); err != nil { t.Fatal(err) }
		}
		entries, err := node.config.Storage.LoadLog()
		if err != nil || len(entries) > 4 { t.Fatalf("unbounded suffix after %d writes: %d, %v", i, len(entries), err) }
	}
	c.reconnect(follower)
	msg := receiveApply(t, c.nodes[follower].(*electionNode).applied)
	if !msg.SnapshotValid || msg.SnapshotIndex != 30 || string(msg.Snapshot) != strings.Join(state, ",") { t.Fatalf("follower did not install snapshot: %+v", msg) }
	index := submitCommand(t, c, leader, "after-snapshot")
	c.waitCommitted(index, 3)
	msg = receiveApply(t, c.nodes[follower].(*electionNode).applied)
	if msg.SnapshotValid || msg.Index != index || string(msg.Command) != "after-snapshot" { t.Fatalf("follower suffix: %+v", msg) }
}

func TestDiskSnapshotRestartAndSuffix(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	network := transport.NewNetwork(1)
	newNode := func(store Storage, applied chan ApplyMsg) *Node {
		node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: store, ApplyCh: applied, ElectionMin: 30 * time.Millisecond, ElectionMax: 30 * time.Millisecond, Heartbeat: 10 * time.Millisecond})
		if err != nil { t.Fatal(err) }
		network.Register(1, node)
		node.Run()
		return node
	}
	waitLeader := func(node *Node) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			status, err := node.Status(context.Background())
			if err != nil { t.Fatal(err) }
			if status.Role == Leader { return }
			time.Sleep(time.Millisecond)
		}
		t.Fatal("single node did not become leader")
	}
	applied := make(chan ApplyMsg, 10)
	node := newNode(store, applied)
	t.Cleanup(func() { if node != nil { node.Stop() }; if store != nil { _ = store.Close() } })
	waitLeader(node)
	for i, command := range []string{"one", "two", "three"} {
		index, _, leader := node.Start([]byte(command))
		if !leader || index != uint64(i+1) { t.Fatalf("proposal index=%d leader=%v", index, leader) }
		msg := receiveApply(t, applied)
		if msg.Index != index || string(msg.Command) != command { t.Fatalf("apply: %+v", msg) }
	}
	if err := node.Snapshot(2, []byte("one,two")); err != nil { t.Fatal(err) }
	node.Stop()
	if err := store.Close(); err != nil { t.Fatal(err) }
	store, err = OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	applied = make(chan ApplyMsg, 10)
	node = newNode(store, applied)
	msg := receiveApply(t, applied)
	if !msg.SnapshotValid || msg.SnapshotIndex != 2 || string(msg.Snapshot) != "one,two" { t.Fatalf("restore: %+v", msg) }
	waitLeader(node)
	index, _, leader := node.Start([]byte("four"))
	if !leader || index != 4 { t.Fatalf("proposal index=%d leader=%v", index, leader) }
	third, fourth := receiveApply(t, applied), receiveApply(t, applied)
	if third.Index != 3 || string(third.Command) != "three" || fourth.Index != 4 || string(fourth.Command) != "four" { t.Fatalf("suffix: %+v %+v", third, fourth) }
}

func TestDiskSnapshotReplacesOldLog(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	entries := make([]transport.LogEntry, 80)
	for i := range entries { entries[i] = transport.LogEntry{Term: 1, Command: bytes.Repeat([]byte("x"), 256)} }
	if err := store.SaveLog(entries); err != nil { t.Fatal(err) }
	before, err := os.Stat(filepath.Join(dir, "log"))
	if err != nil { t.Fatal(err) }
	if err := store.SaveSnapshot(SnapshotState{Index: 79, Term: 1, Data: []byte("image")}, entries[79:]); err != nil { t.Fatal(err) }
	after, err := os.Stat(filepath.Join(dir, "log"))
	if err != nil || after.Size() >= before.Size() { t.Fatalf("log not compacted: before=%d after=%v err=%v", before.Size(), after, err) }
	if err := store.Close(); err != nil { t.Fatal(err) }
	store, err = OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	defer func() { if err := store.Close(); err != nil { t.Error(err) } }()
	snapshot, err := store.LoadSnapshot()
	if err != nil || snapshot.Index != 79 || snapshot.Term != 1 || string(snapshot.Data) != "image" { t.Fatalf("snapshot %+v, %v", snapshot, err) }
	suffix, err := store.LoadLog()
	if err != nil || len(suffix) != 1 || !bytes.Equal(suffix[0].Command, entries[79].Command) { t.Fatalf("suffix %+v, %v", suffix, err) }
}

func TestInstallSnapshotRejectsStaleAndRestoresSuffix(t *testing.T) {
	network := transport.NewNetwork(1)
	store := &MemoryStorage{}
	applied := make(chan ApplyMsg, 8)
	node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: store, ApplyCh: applied, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	ctx := context.Background()
	_, err = node.InstallSnapshot(ctx, transport.InstallSnapshotRequest{Term: 2, LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("state"), Done: true})
	if err != nil { t.Fatal(err) }
	msg := receiveApply(t, applied)
	if !msg.SnapshotValid || msg.SnapshotIndex != 10 || string(msg.Snapshot) != "state" { t.Fatalf("snapshot apply: %+v", msg) }
	_, err = node.InstallSnapshot(ctx, transport.InstallSnapshotRequest{Term: 1, LastIncludedIndex: 20, LastIncludedTerm: 1, Data: []byte("stale"), Done: true})
	if err != nil { t.Fatal(err) }
	response, err := node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: 2, PrevLogIndex: 9, PrevLogTerm: 1})
	if err != nil || response.Success || !response.Compacted || response.ConflictIndex != 11 { t.Fatalf("compaction hint: %+v %v", response, err) }
	response, err = node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: 2, PrevLogIndex: 10, PrevLogTerm: 1, Entries: []transport.LogEntry{{Term: 2, Command: []byte("next")}}, LeaderCommit: 11})
	if err != nil || !response.Success { t.Fatalf("append after snapshot: %+v %v", response, err) }
	msg = receiveApply(t, applied)
	if msg.SnapshotValid || msg.Index != 11 || string(msg.Command) != "next" { t.Fatalf("suffix apply: %+v", msg) }
	snapshot, err := store.LoadSnapshot()
	if err != nil || snapshot.Index != 10 || string(snapshot.Data) != "state" { t.Fatalf("stored snapshot: %+v %v", snapshot, err) }
}
