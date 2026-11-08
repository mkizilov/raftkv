package main

import (
	"context"
	"io"
	"testing"

	"github.com/you/raftkv/client"
)

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{}, {"get"}, {"put", "key"}, {"cas", "key", "bad", "value"}, {"bench", "0", "key"}, {"-client-id", "client", "get", "key"}, {"member", "add", "4"}, {"member", "remove", "1"}, {"watch"}, {"-after-revision", "bad", "watch", "prefix"}, {"unknown"}} {
		if err := run(context.Background(), args, io.Discard, io.Discard); err == nil { t.Fatalf("accepted %v", args) }
	}
}

func TestConfiguredMembers(t *testing.T) {
	states := []client.NodeStatus{
		{NodeID: 2, Address: "node-2", Role: "leader", Term: 4, VoterIDs: []uint64{1, 2, 3}},
		{NodeID: 1, Address: "node-1", Role: "follower", Term: 4},
		{Address: "node-3", Err: io.EOF},
	}
	members, err := configuredMembers(states)
	if err != nil || len(members) != 3 || members[0].NodeID != 1 || !members[0].Reachable || members[1].Role != "leader" || members[2].Reachable { t.Fatalf("members: %+v %v", members, err) }
	states = append(states, client.NodeStatus{NodeID: 3, Role: "leader", Term: 4, VoterIDs: []uint64{1, 2, 3}})
	if _, err := configuredMembers(states); err == nil { t.Fatal("accepted two leaders in one term") }
}
