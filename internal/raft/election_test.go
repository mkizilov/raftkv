package raft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

type electionNode struct { *Node; t *testing.T; applied chan ApplyMsg }

func (n *electionNode) start() { n.Run() }
func (n *electionNode) stop() { n.Stop() }
func (n *electionNode) status() (uint64, bool, uint64) {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := n.Status(ctx)
	if err != nil { n.t.Fatalf("node status: %v", err) }
	return status.Term, status.Role == Leader, status.CommitIndex
}

func electionCluster(t *testing.T, count int) *testCluster {
	t.Helper()
	seed := int64(314159)
	if value := os.Getenv("RAFTKV_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil { t.Fatal(err) }
	}
	return newTestCluster(t, count, func(id transport.NodeID, tr transport.Transport) clusterNode {
		peers := make([]transport.NodeID, 0, count-1)
		for i := 1; i <= count; i++ { if transport.NodeID(i) != id { peers = append(peers, transport.NodeID(i)) } }
		applied := make(chan ApplyMsg, 128)
		node, err := NewNode(Config{ID: id, Peers: peers, Transport: tr, Storage: &MemoryStorage{}, Seed: seed + int64(id), ApplyCh: applied})
		if err != nil { t.Fatal(err) }
		return &electionNode{Node: node, t: t, applied: applied}
	})
}

// sampleLeaders remembers every observed leader by term, including isolated
// nodes. Sampling is a regression check, not an exhaustive proof of safety.
func sampleLeaders(t *testing.T, c *testCluster, seen map[uint64]int) {
	t.Helper()
	for i, node := range c.nodes {
		term, leader, _ := node.status()
		if !leader { continue }
		if previous, ok := seen[term]; ok && previous != i {
			t.Fatalf("leaders %d and %d observed in term %d", previous+1, i+1, term)
		}
		seen[term] = i
	}
}

func TestElectionAndPartition(t *testing.T) {
	for _, count := range []int{3, 5} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			c := electionCluster(t, count)
			old := c.checkOneLeader()
			seen := make(map[uint64]int)
			sampleLeaders(t, c, seen)
			oldTerm, _, _ := c.nodes[old].status()
			c.disconnect(old)
			deadline := time.Now().Add(3 * time.Second)
			newLeader := -1
			for time.Now().Before(deadline) {
				sampleLeaders(t, c, seen)
				for i, node := range c.nodes {
					term, leader, _ := node.status()
					if i != old && leader && term > oldTerm { newLeader = i }
				}
				if newLeader >= 0 { break }
				time.Sleep(5 * time.Millisecond)
			}
			if newLeader < 0 { t.Fatal("majority failed to elect a new leader") }
			c.reconnect(old)
			deadline = time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				sampleLeaders(t, c, seen)
				term, leader, _ := c.nodes[old].status()
				if !leader && term > oldTerm { break }
				time.Sleep(5 * time.Millisecond)
			}
			term, leader, _ := c.nodes[old].status()
			if leader || term <= oldTerm { t.Fatalf("old leader did not step down: term=%d leader=%v", term, leader) }
			c.checkOneLeader()
			until := time.Now().Add(350 * time.Millisecond)
			for time.Now().Before(until) { sampleLeaders(t, c, seen); time.Sleep(5 * time.Millisecond) }
		})
	}
}

func TestRequestVotePersistenceAndLogFreshness(t *testing.T) {
	network := transport.NewNetwork(9)
	store := &MemoryStorage{}
	entries := make([]transport.LogEntry, 7)
	for i := range entries { entries[i].Term = 3 }
	if err := store.SaveLog(entries); err != nil { t.Fatal(err) }
	config := Config{ID: 1, Transport: network.Endpoint(1), Storage: store, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute}
	node, err := NewNode(config)
	if err != nil { t.Fatal(err) }
	node.Run()
	t.Cleanup(func() { node.Stop() })
	request := func(term, index, logTerm uint64, candidate transport.NodeID) transport.RequestVoteResponse {
		t.Helper()
		response, err := node.RequestVote(context.Background(), transport.RequestVoteRequest{Term: term, CandidateID: candidate, LastLogIndex: index, LastLogTerm: logTerm})
		if err != nil { t.Fatal(err) }
		return response
	}
	if request(1, 100, 2, 2).VoteGranted { t.Fatal("granted vote to older log term") }
	if request(1, 6, 3, 2).VoteGranted { t.Fatal("granted vote to shorter log") }
	if !request(1, 7, 3, 2).VoteGranted { t.Fatal("refused up-to-date candidate") }
	state, err := store.Load()
	if err != nil || state.Term != 1 || !state.HasVote || state.VotedFor != 2 { t.Fatalf("persisted state: %+v, %v", state, err) }
	if request(1, 8, 3, 3).VoteGranted { t.Fatal("voted twice in same term") }
	node.Stop()
	node, err = NewNode(config)
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	if request(1, 8, 3, 3).VoteGranted { t.Fatal("forgot vote after restart") }
	if !request(2, 1, 4, 3).VoteGranted { t.Fatal("rejected newer log term") }
	if request(1, 100, 10, 2).VoteGranted { t.Fatal("granted stale term") }
}

type failingStorage struct{}
func (failingStorage) Load() (HardState, error) { return HardState{}, nil }
func (failingStorage) Save(HardState) error { return errors.New("disk failure") }
func (failingStorage) LoadLog() ([]transport.LogEntry, error) { return nil, nil }
func (failingStorage) SaveLog([]transport.LogEntry) error { return errors.New("disk failure") }

func TestPersistenceFailureStopsNode(t *testing.T) {
	network := transport.NewNetwork(1)
	node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: failingStorage{}})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	_, err = node.RequestVote(context.Background(), transport.RequestVoteRequest{Term: 1, CandidateID: 2})
	if err == nil || err.Error() != "raft: persist state: disk failure" { t.Fatalf("persistence error: %v", err) }
}

func TestHeartbeatUpdatesTermAndRejectsMissingPrefix(t *testing.T) {
	network := transport.NewNetwork(1)
	store := &MemoryStorage{}
	node, err := NewNode(Config{ID: 1, Transport: network.Endpoint(1), Storage: store, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	ctx := context.Background()
	response, err := node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: 3, LeaderID: 2})
	if err != nil || !response.Success || response.Term != 3 { t.Fatalf("heartbeat: %+v, %v", response, err) }
	state, err := store.Load()
	if err != nil || state.Term != 3 || state.HasVote { t.Fatalf("persisted state: %+v, %v", state, err) }
	response, err = node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: 2, LeaderID: 2})
	if err != nil || response.Success || response.Term != 3 { t.Fatalf("stale heartbeat: %+v, %v", response, err) }
	response, err = node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: 3, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 3})
	if err != nil || response.Success { t.Fatalf("missing prefix: %+v, %v", response, err) }
}
