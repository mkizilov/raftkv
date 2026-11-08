package transport

import "context"

// NodeID identifies a server within a cluster.
type NodeID uint64

type LogEntry struct {
	Term    uint64
	Command []byte
}

type RequestVoteRequest struct {
	Term, LastLogIndex, LastLogTerm uint64
	CandidateID                    NodeID
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term, PrevLogIndex, PrevLogTerm, LeaderCommit uint64
	LeaderID                                      NodeID
	Entries                                       []LogEntry
}

type AppendEntriesResponse struct {
	Term              uint64
	Success           bool
	ConflictTerm      uint64
	ConflictIndex     uint64
	HasConflictTerm   bool
}

// InstallSnapshot carries one snapshot chunk. Offset and Done allow streaming
// implementations without requiring an entire snapshot in a single message.
type InstallSnapshotRequest struct {
	Term, LastIncludedIndex, LastIncludedTerm, Offset uint64
	LeaderID                                          NodeID
	Data                                              []byte
	Done                                              bool
}

type InstallSnapshotResponse struct{ Term uint64 }

// Transport is bound to the sending node; all calls honor context cancellation.
type Transport interface {
	RequestVote(context.Context, NodeID, RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(context.Context, NodeID, AppendEntriesRequest) (AppendEntriesResponse, error)
	InstallSnapshot(context.Context, NodeID, InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// Handler processes messages delivered to a node.
type Handler interface {
	RequestVote(context.Context, RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(context.Context, AppendEntriesRequest) (AppendEntriesResponse, error)
	InstallSnapshot(context.Context, InstallSnapshotRequest) (InstallSnapshotResponse, error)
}
