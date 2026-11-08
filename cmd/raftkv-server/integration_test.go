package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestThreeNodeGRPCCluster(t *testing.T) {
	raftPeers, clientPeers := make(map[transport.NodeID]string), make(map[transport.NodeID]string)
	for i := 0; i < 3; i++ {
		id := transport.NodeID(i+1)
		raftPeers[id] = fmt.Sprintf("127.0.0.1:%d", 21860+i*2)
		clientPeers[id] = fmt.Sprintf("127.0.0.1:%d", 21861+i*2)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		id := transport.NodeID(i+1)
		c := config{id: id, raftListen: raftPeers[id], clientListen: clientPeers[id], httpListen: fmt.Sprintf("127.0.0.1:%d", 21866+i), dataDir: t.TempDir(), raftPeers: raftPeers, clientPeers: clientPeers}
		go func() { errs <- serve(ctx, c) }()
	}
	t.Cleanup(func() {
		cancel()
		for i := 0; i < 3; i++ { if err := <-errs; err != nil { t.Errorf("server: %v", err) } }
	})
	clients := make([]pb.KVServiceClient, 3)
	for i := range clients {
		conn, err := grpc.NewClient("passthrough:///"+clientPeers[transport.NodeID(i+1)], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil { t.Fatal(err) }
		t.Cleanup(func() { if err := conn.Close(); err != nil { t.Error(err) } })
		clients[i] = pb.NewKVServiceClient(conn)
	}
	request := &pb.PutRequest{Identity: &pb.RequestIdentity{ClientId: "integration", Sequence: 1}, Key: "key", Value: "value"}
	leader := -1
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && leader < 0 {
		for i, client := range clients {
			call, stop := context.WithTimeout(ctx, 500*time.Millisecond)
			response, err := client.Put(call, request)
			stop()
			if err == nil {
				if response.Version != 1 || !response.Found { t.Fatalf("put: %+v", response) }
				leader = i
				break
			}
			if code := status.Code(err); code != codes.FailedPrecondition && code != codes.Unavailable && code != codes.DeadlineExceeded { t.Fatal(err) }
		}
		if leader < 0 { time.Sleep(20 * time.Millisecond) }
	}
	if leader < 0 { t.Fatal("no leader accepted a write") }
	statusCtx, stopStatus := context.WithTimeout(ctx, 2*time.Second)
	memberStatus, err := clients[leader].Status(statusCtx, &pb.StatusRequest{})
	stopStatus()
	if err != nil || !reflect.DeepEqual(memberStatus.GetVoterIds(), []uint64{1, 2, 3}) { t.Fatalf("configured voters: %+v %v", memberStatus, err) }
	readCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	response, err := clients[leader].Get(readCtx, &pb.GetRequest{Identity: &pb.RequestIdentity{ClientId: "integration", Sequence: 2}, Key: "key"})
	stop()
	if err != nil || response.Value != "value" || response.Version != 1 { t.Fatalf("read: %+v %v", response, err) }
	probe := &http.Client{Timeout: time.Second}
	httpAddress := fmt.Sprintf("http://127.0.0.1:%d", 21866+leader)
	deadline = time.Now().Add(3*time.Second)
	ready := false
	for time.Now().Before(deadline) {
		r, err := probe.Get(httpAddress + "/readyz")
		if err == nil { _ = r.Body.Close(); if r.StatusCode == http.StatusOK { ready = true; break } }
		time.Sleep(20 * time.Millisecond)
	}
	if !ready { t.Fatal("leader did not become ready") }
	r, err := probe.Get(httpAddress + "/metrics")
	if err != nil { t.Fatal(err) }
	metrics, err := io.ReadAll(r.Body)
	closeErr := r.Body.Close()
	if err != nil || closeErr != nil || r.StatusCode != http.StatusOK { t.Fatalf("metrics: %v %v status %d", err, closeErr, r.StatusCode) }
	for _, name := range []string{"raftkv_current_term", "raftkv_role", "raftkv_commit_index", "raftkv_applied_index", "raftkv_rpc_requests_total", "raftkv_rpc_duration_seconds_bucket"} {
		if !strings.Contains(string(metrics), name) { t.Errorf("missing metric %s", name) }
	}
	follower := (leader + 1) % 3
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		call, stop := context.WithTimeout(ctx, 500*time.Millisecond)
		_, err = clients[follower].Put(call, &pb.PutRequest{Identity: &pb.RequestIdentity{ClientId: "integration", Sequence: 3}, Key: "key", Value: "other"})
		stop()
		if status.Code(err) != codes.FailedPrecondition { t.Fatalf("follower write: %v", err) }
		st, ok := status.FromError(err)
		if !ok { t.Fatalf("not a gRPC status: %v", err) }
		for _, detail := range st.Details() {
			if hint, ok := detail.(*pb.NotLeaderDetail); ok && hint.LeaderAddress == clientPeers[transport.NodeID(leader+1)] { return }
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(errors.New("follower did not report leader client address"))
}
