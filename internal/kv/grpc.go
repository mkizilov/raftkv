package kv

import (
	"context"
	"errors"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPC exposes a KV server; leader addresses are client-facing addresses.
type GRPC struct {
	pb.UnimplementedKVServiceServer
	Server *Server
	NodeID transport.NodeID
	LeaderAddresses map[transport.NodeID]string
}

func (g *GRPC) response(ctx context.Context, r Result, err error) (*pb.KVResponse, error) {
	if err == nil { return &pb.KVResponse{Value: r.Value, Version: r.Version, Found: r.Found, Swapped: r.Swapped}, nil }
	switch {
	case errors.Is(err, ErrNotLeader):
		state, statusErr := g.Server.Node().Status(ctx)
		if statusErr != nil { return nil, status.Error(codes.Unavailable, statusErr.Error()) }
		address := ""
		if state.LeaderKnown { address = g.LeaderAddresses[state.LeaderID] }
		detail, detailErr := status.New(codes.FailedPrecondition, "not leader").WithDetails(&pb.NotLeaderDetail{LeaderAddress: address})
		if detailErr != nil { return nil, status.Error(codes.Internal, detailErr.Error()) }
		return nil, detail.Err()
	case errors.Is(err, ErrInvalidRequest): return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrRequestConflict): return nil, status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrVersionExhausted): return nil, status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled): return nil, status.FromContextError(err).Err()
	case errors.Is(err, ErrStopped), errors.Is(err, raft.ErrStopped): return nil, status.Error(codes.Unavailable, err.Error())
	default: return nil, status.Error(codes.Internal, err.Error())
	}
}

func (g *GRPC) Get(ctx context.Context, req *pb.GetRequest) (*pb.KVResponse, error) {
	id := req.GetIdentity()
	r, err := g.Server.Get(ctx, id.GetClientId(), id.GetSequence(), req.GetKey())
	return g.response(ctx, r, err)
}

func (g *GRPC) Put(ctx context.Context, req *pb.PutRequest) (*pb.KVResponse, error) {
	id := req.GetIdentity()
	r, err := g.Server.Put(ctx, id.GetClientId(), id.GetSequence(), req.GetKey(), req.GetValue())
	return g.response(ctx, r, err)
}

func (g *GRPC) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.KVResponse, error) {
	id := req.GetIdentity()
	r, err := g.Server.Delete(ctx, id.GetClientId(), id.GetSequence(), req.GetKey())
	return g.response(ctx, r, err)
}

func (g *GRPC) CompareAndSwap(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.KVResponse, error) {
	id := req.GetIdentity()
	r, err := g.Server.CompareAndSwap(ctx, id.GetClientId(), id.GetSequence(), req.GetKey(), req.GetExpectedVersion(), req.GetValue())
	return g.response(ctx, r, err)
}

func (g *GRPC) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	state, err := g.Server.Node().Status(ctx)
	if err != nil { return nil, status.Error(codes.Unavailable, err.Error()) }
	role := "follower"
	switch state.Role {
	case raft.Candidate: role = "candidate"
	case raft.Leader: role = "leader"
	}
	address := ""
	if state.LeaderKnown { address = g.LeaderAddresses[state.LeaderID] }
	return &pb.StatusResponse{NodeId: uint64(g.NodeID), Role: role, Term: state.Term, CommitIndex: state.CommitIndex, LeaderKnown: state.LeaderKnown, LeaderId: uint64(state.LeaderID), LeaderAddress: address}, nil
}
