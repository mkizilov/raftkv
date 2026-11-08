package raft

import (
	"context"
	"testing"

	"github.com/you/raftkv/internal/transport"
)

type fakeNode struct {
	term, committed uint64
	leader          bool
}

func (*fakeNode) start() {}
func (*fakeNode) stop()  {}
func (n *fakeNode) status() (uint64, bool, uint64) { return n.term, n.leader, n.committed }
func (*fakeNode) RequestVote(context.Context, transport.RequestVoteRequest) (transport.RequestVoteResponse, error) {
	return transport.RequestVoteResponse{}, nil
}
func (*fakeNode) AppendEntries(context.Context, transport.AppendEntriesRequest) (transport.AppendEntriesResponse, error) {
	return transport.AppendEntriesResponse{}, nil
}
func (*fakeNode) InstallSnapshot(context.Context, transport.InstallSnapshotRequest) (transport.InstallSnapshotResponse, error) {
	return transport.InstallSnapshotResponse{}, nil
}

func TestClusterHelper(t *testing.T) {
	c := newTestCluster(t, 3, func(id transport.NodeID, _ transport.Transport) clusterNode {
		return &fakeNode{term: 2, leader: id == 1, committed: 4}
	})
	if got := c.checkOneLeader(); got != 0 { t.Fatalf("leader index = %d", got) }
	if got := c.checkTerms(); got != 2 { t.Fatalf("term = %d", got) }
	c.waitCommitted(4, 3)
	c.disconnect(0)
	c.checkNoLeader()
	c.reconnect(0)
	if got := c.checkOneLeader(); got != 0 { t.Fatalf("leader index = %d", got) }
}
