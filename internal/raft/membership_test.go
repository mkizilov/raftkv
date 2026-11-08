package raft

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/you/raftkv/internal/transport"
)

func TestConfiguredVotersRejectUnknownCandidate(t *testing.T) {
	network := transport.NewNetwork(1)
	store := &MemoryStorage{}
	node, err := NewNode(Config{ID: 2, Peers: []transport.NodeID{3, 1}, Transport: network.Endpoint(2), Storage: store, ElectionMin: time.Hour, ElectionMax: time.Hour, Heartbeat: time.Minute})
	if err != nil { t.Fatal(err) }
	node.Run()
	defer node.Stop()
	ctx := context.Background()
	reply, err := node.RequestVote(ctx, transport.RequestVoteRequest{Term: 100, CandidateID: 4})
	if err != nil || reply.Term != 0 || reply.VoteGranted { t.Fatalf("unknown candidate: %+v %v", reply, err) }
	hard, err := store.Load()
	if err != nil || hard.Term != 0 || hard.HasVote { t.Fatalf("state changed for unknown voter: %+v %v", hard, err) }
	reply, err = node.RequestVote(ctx, transport.RequestVoteRequest{Term: 1, CandidateID: 1})
	if err != nil || reply.Term != 1 || !reply.VoteGranted { t.Fatalf("configured voter: %+v %v", reply, err) }
	state, err := node.Status(ctx)
	if err != nil || !reflect.DeepEqual(state.Voters, []transport.NodeID{1, 2, 3}) { t.Fatalf("voters: %+v %v", state.Voters, err) }
	state.Voters[0] = 99
	again, err := node.Status(ctx)
	if err != nil || !reflect.DeepEqual(again.Voters, []transport.NodeID{1, 2, 3}) { t.Fatalf("voters alias internal state: %+v %v", again.Voters, err) }
}
