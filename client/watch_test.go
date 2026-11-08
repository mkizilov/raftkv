package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type watchFake struct {
	pb.UnimplementedKVServiceServer
	revision uint64
	compacted bool
}

func (s *watchFake) Watch(req *pb.WatchRequest, stream pb.KVService_WatchServer) error {
	if s.compacted {
		st, err := status.New(codes.OutOfRange, "re-read full state").WithDetails(&pb.WatchCompactedDetail{CompactedRevision: 12})
		if err != nil { return err }
		return st.Err()
	}
	if req.AfterRevision != s.revision-1 { return status.Errorf(codes.InvalidArgument, "cursor=%d", req.AfterRevision) }
	if err := stream.Send(&pb.WatchEvent{Revision: s.revision, Key: "app/key", Value: "value", Version: s.revision}); err != nil { return err }
	return status.Error(codes.Unavailable, "leader disconnected")
}

func startWatchFake(t *testing.T, address string, handler *watchFake) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil { t.Fatal(err) }
	server := grpc.NewServer()
	pb.RegisterKVServiceServer(server, handler)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); if err := <-done; err != nil { t.Error(err) } })
}

func TestWatchReconnectsFromLastDeliveredRevision(t *testing.T) {
	startWatchFake(t, "127.0.0.1:21871", &watchFake{revision: 1})
	startWatchFake(t, "127.0.0.1:21872", &watchFake{revision: 2})
	c, err := New(Config{Endpoints: []string{"127.0.0.1:21871", "127.0.0.1:21872"}})
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := c.Close(); err != nil { t.Error(err) } })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stop := errors.New("done")
	var events []WatchEvent
	err = c.Watch(ctx, "app/", 0, func(event WatchEvent) error {
		events = append(events, event)
		if len(events) == 2 { return stop }
		return nil
	})
	if !errors.Is(err, stop) || len(events) != 2 || events[0].Revision != 1 || events[1].Revision != 2 { t.Fatalf("events %+v, error %v", events, err) }
}

func TestWatchCompactionError(t *testing.T) {
	startWatchFake(t, "127.0.0.1:21873", &watchFake{compacted: true})
	c, err := New(Config{Endpoints: []string{"127.0.0.1:21873"}})
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := c.Close(); err != nil { t.Error(err) } })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = c.Watch(ctx, "", 0, func(WatchEvent) error { t.Error("unexpected event"); return nil })
	var compacted *WatchCompactedError
	if !errors.As(err, &compacted) || compacted.Revision != 12 { t.Fatalf("compacted error: %v", err) }
}
