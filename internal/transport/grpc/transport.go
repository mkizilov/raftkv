// Package grpctransport implements Raft RPCs over reusable gRPC connections.
package grpctransport

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxMessageSize = 64 << 20

type Transport struct {
	connections map[transport.NodeID]*grpc.ClientConn
	clients map[transport.NodeID]pb.RaftServiceClient
	timeout time.Duration
}

func New(peers map[transport.NodeID]string, timeout time.Duration) (*Transport, error) {
	if timeout <= 0 { return nil, errors.New("transport: timeout must be positive") }
	t := &Transport{connections: make(map[transport.NodeID]*grpc.ClientConn), clients: make(map[transport.NodeID]pb.RaftServiceClient), timeout: timeout}
	for id, address := range peers {
		if address == "" { _ = t.Close(); return nil, fmt.Errorf("transport: empty address for node %d", id) }
		conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)))
		if err != nil { _ = t.Close(); return nil, err }
		t.connections[id] = conn
		t.clients[id] = pb.NewRaftServiceClient(conn)
	}
	return t, nil
}

func (t *Transport) Close() error {
	var result error
	for _, conn := range t.connections { result = errors.Join(result, conn.Close()) }
	return result
}

func (t *Transport) client(id transport.NodeID) (pb.RaftServiceClient, error) {
	client, ok := t.clients[id]
	if !ok { return nil, fmt.Errorf("transport: unknown node %d", id) }
	return client, nil
}

func (t *Transport) RequestVote(ctx context.Context, id transport.NodeID, req transport.RequestVoteRequest) (transport.RequestVoteResponse, error) {
	client, err := t.client(id)
	if err != nil { return transport.RequestVoteResponse{}, err }
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	r, err := client.RequestVote(ctx, &pb.RequestVoteRequest{Term: req.Term, CandidateId: uint64(req.CandidateID), LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm})
	if err != nil { return transport.RequestVoteResponse{}, err }
	return transport.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

func (t *Transport) AppendEntries(ctx context.Context, id transport.NodeID, req transport.AppendEntriesRequest) (transport.AppendEntriesResponse, error) {
	client, err := t.client(id)
	if err != nil { return transport.AppendEntriesResponse{}, err }
	entries := make([]*pb.LogEntry, len(req.Entries))
	for i, entry := range req.Entries { entries[i] = &pb.LogEntry{Term: entry.Term, Command: entry.Command} }
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	r, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{Term: req.Term, LeaderId: uint64(req.LeaderID), PrevLogIndex: req.PrevLogIndex, PrevLogTerm: req.PrevLogTerm, Entries: entries, LeaderCommit: req.LeaderCommit})
	if err != nil { return transport.AppendEntriesResponse{}, err }
	return transport.AppendEntriesResponse{Term: r.Term, Success: r.Success, ConflictTerm: r.ConflictTerm, ConflictIndex: r.ConflictIndex, HasConflictTerm: r.HasConflictTerm, Compacted: r.Compacted}, nil
}

func (t *Transport) InstallSnapshot(ctx context.Context, id transport.NodeID, req transport.InstallSnapshotRequest) (transport.InstallSnapshotResponse, error) {
	client, err := t.client(id)
	if err != nil { return transport.InstallSnapshotResponse{}, err }
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	r, err := client.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{Term: req.Term, LeaderId: uint64(req.LeaderID), LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm, Offset: req.Offset, Data: req.Data, Done: req.Done})
	if err != nil { return transport.InstallSnapshotResponse{}, err }
	return transport.InstallSnapshotResponse{Term: r.Term}, nil
}

// Handler adapts a Raft node to its protobuf RPC service.
type Handler struct {
	pb.UnimplementedRaftServiceServer
	Node transport.Handler
}

func (h *Handler) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r, err := h.Node.RequestVote(ctx, transport.RequestVoteRequest{Term: req.Term, CandidateID: transport.NodeID(req.CandidateId), LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm})
	if err != nil { return nil, err }
	return &pb.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

func (h *Handler) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]transport.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		if entry == nil { return nil, status.Error(codes.InvalidArgument, "nil log entry") }
		entries[i] = transport.LogEntry{Term: entry.Term, Command: entry.Command}
	}
	r, err := h.Node.AppendEntries(ctx, transport.AppendEntriesRequest{Term: req.Term, LeaderID: transport.NodeID(req.LeaderId), PrevLogIndex: req.PrevLogIndex, PrevLogTerm: req.PrevLogTerm, Entries: entries, LeaderCommit: req.LeaderCommit})
	if err != nil { return nil, err }
	return &pb.AppendEntriesResponse{Term: r.Term, Success: r.Success, ConflictTerm: r.ConflictTerm, ConflictIndex: r.ConflictIndex, HasConflictTerm: r.HasConflictTerm, Compacted: r.Compacted}, nil
}

func (h *Handler) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	r, err := h.Node.InstallSnapshot(ctx, transport.InstallSnapshotRequest{Term: req.Term, LeaderID: transport.NodeID(req.LeaderId), LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm, Offset: req.Offset, Data: req.Data, Done: req.Done})
	if err != nil { return nil, err }
	return &pb.InstallSnapshotResponse{Term: r.Term}, nil
}
