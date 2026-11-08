package grpctransport

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type echoHandler struct{}

func (echoHandler) RequestVote(ctx context.Context, req transport.RequestVoteRequest) (transport.RequestVoteResponse, error) {
	if req.Term == 99 { <-ctx.Done(); return transport.RequestVoteResponse{}, ctx.Err() }
	return transport.RequestVoteResponse{Term: req.Term, VoteGranted: req.CandidateID == 5 && req.LastLogIndex == 7 && req.LastLogTerm == 3}, nil
}
func (echoHandler) AppendEntries(_ context.Context, req transport.AppendEntriesRequest) (transport.AppendEntriesResponse, error) {
	if req.LeaderID != 5 || req.PrevLogIndex != 6 || req.PrevLogTerm != 2 || req.LeaderCommit != 4 || !reflect.DeepEqual(req.Entries, []transport.LogEntry{{Term: 3, Command: []byte("entry")}}) { return transport.AppendEntriesResponse{}, errors.New("bad append request") }
	return transport.AppendEntriesResponse{Term: req.Term, ConflictTerm: 3, ConflictIndex: 2, HasConflictTerm: true, Compacted: true}, nil
}
func (echoHandler) InstallSnapshot(_ context.Context, req transport.InstallSnapshotRequest) (transport.InstallSnapshotResponse, error) {
	if req.LeaderID != 5 || req.LastIncludedIndex != 8 || req.LastIncludedTerm != 3 || req.Offset != 0 || string(req.Data) != "snapshot" || !req.Done { return transport.InstallSnapshotResponse{}, errors.New("bad snapshot request") }
	return transport.InstallSnapshotResponse{Term: req.Term}, nil
}

func TestRPCTransportRoundTripAndTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:21870")
	if err != nil { t.Fatal(err) }
	server := grpc.NewServer()
	pb.RegisterRaftServiceServer(server, &Handler{Node: echoHandler{}})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); if err := <-done; err != nil { t.Error(err) } })
	client, err := New(map[transport.NodeID]string{2: "127.0.0.1:21870"}, 250*time.Millisecond)
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := client.Close(); err != nil { t.Error(err) } })
	ctx := context.Background()
	vote, err := client.RequestVote(ctx, 2, transport.RequestVoteRequest{Term: 4, CandidateID: 5, LastLogIndex: 7, LastLogTerm: 3})
	if err != nil || vote.Term != 4 || !vote.VoteGranted { t.Fatalf("vote: %+v %v", vote, err) }
	appendResult, err := client.AppendEntries(ctx, 2, transport.AppendEntriesRequest{Term: 4, LeaderID: 5, PrevLogIndex: 6, PrevLogTerm: 2, LeaderCommit: 4, Entries: []transport.LogEntry{{Term: 3, Command: []byte("entry")}}})
	if err != nil || appendResult.Term != 4 || appendResult.ConflictTerm != 3 || appendResult.ConflictIndex != 2 || !appendResult.HasConflictTerm || !appendResult.Compacted { t.Fatalf("append: %+v %v", appendResult, err) }
	snapshot, err := client.InstallSnapshot(ctx, 2, transport.InstallSnapshotRequest{Term: 4, LeaderID: 5, LastIncludedIndex: 8, LastIncludedTerm: 3, Data: []byte("snapshot"), Done: true})
	if err != nil || snapshot.Term != 4 { t.Fatalf("snapshot: %+v %v", snapshot, err) }
	if _, err := client.RequestVote(ctx, 2, transport.RequestVoteRequest{Term: 99}); status.Code(err) != codes.DeadlineExceeded { t.Fatalf("timeout: %v", err) }
	if _, err := client.RequestVote(ctx, 3, transport.RequestVoteRequest{}); err == nil { t.Fatal("unknown peer accepted") }
}
