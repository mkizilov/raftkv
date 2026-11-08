package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/client"
	"github.com/you/raftkv/internal/kv"
	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/transport"
	"go.opentelemetry.io/otel/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestMetricsHealthLogsAndTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { if err := provider.Shutdown(context.Background()); err != nil { t.Error(err) } }()
	var logs bytes.Buffer
	tm := New(7, 0, &logs, provider)
	tm.RaftState(raft.Status{Term: 4, Role: raft.Leader, CommitIndex: 9, LeaderKnown: true, LeaderID: 7})
	tm.Applied(8)
	tm.SnapshotSize(123)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "request-1", "traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
	_, err := tm.Unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/raftkv.v1.KVService/Put"}, func(ctx context.Context, _ interface{}) (interface{}, error) {
		trace.SpanFromContext(ctx).AddEvent("kv.result_applied")
		return nil, nil
	})
	if err != nil { t.Fatal(err) }
	var line map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &line); err != nil { t.Fatal(err) }
	if line["node_id"] != float64(7) || line["term"] != float64(4) || line["role"] != "leader" || line["request_id"] != "request-1" { t.Fatalf("log: %+v", line) }
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || len(spans[0].Events) != 1 || spans[0].Events[0].Name != "kv.result_applied" { t.Fatalf("spans: %+v", spans) }
	live, ready := true, false
	handler := tm.Handler(func() bool { return live }, func(context.Context) error { if !ready { return errors.New("no quorum") }; return nil })
	check := func(path string, want int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		if r.Code != want { t.Fatalf("%s: status %d, want %d", path, r.Code, want) }
		return r
	}
	check("/healthz", http.StatusOK)
	check("/readyz", http.StatusServiceUnavailable)
	ready = true
	check("/readyz", http.StatusOK)
	metrics := check("/metrics", http.StatusOK).Body.String()
	for _, want := range []string{"raftkv_current_term", "raftkv_role", "raftkv_commit_index", "raftkv_applied_index", "raftkv_leader_changes_total", "raftkv_snapshot_size_bytes", "raftkv_rpc_requests_total", "raftkv_rpc_duration_seconds_bucket"} {
		if !strings.Contains(metrics, want) { t.Errorf("missing metric %s", want) }
	}
	live = false
	check("/healthz", http.StatusServiceUnavailable)
}

func TestInvalidOTLPEndpoint(t *testing.T) {
	if _, err := NewTracerProvider(context.Background(), ":bad"); err == nil { t.Fatal("accepted invalid endpoint") }
}

func TestLogLevelAndLeaderTransitions(t *testing.T) {
	var logs bytes.Buffer
	tm := New(2, slog.LevelWarn, &logs, nil)
	tm.Logger.Info("hidden")
	tm.RaftState(raft.Status{Term: 1, Role: raft.Follower, LeaderKnown: true, LeaderID: 1})
	tm.RaftState(raft.Status{Term: 1, Role: raft.Follower, LeaderKnown: true, LeaderID: 1})
	tm.RaftState(raft.Status{Term: 2, Role: raft.Leader, LeaderKnown: true, LeaderID: 2})
	tm.Logger.Warn("visible")
	if strings.Contains(logs.String(), "hidden") { t.Fatal("info log ignored configured level") }
	var line map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &line); err != nil { t.Fatal(err) }
	if line["node_id"] != float64(2) || line["term"] != float64(2) || line["role"] != "leader" || line["request_id"] != "-" { t.Fatalf("log: %+v", line) }
	r := httptest.NewRecorder()
	tm.Handler(func() bool { return true }, func(context.Context) error { return nil }).ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(r.Body.String(), `raftkv_leader_changes_total{node_id="2"} 2`) { t.Fatalf("leader changes metric missing: %s", r.Body.String()) }
}

func TestGRPCRequestTracesThroughCommit(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { if err := provider.Shutdown(context.Background()); err != nil { t.Error(err) } }()
	var logs bytes.Buffer
	tm := New(1, 0, &logs, provider)
	network := transport.NewNetwork(1)
	service, err := kv.New(raft.Config{ID: 1, Transport: network.Endpoint(1), Storage: &raft.MemoryStorage{}, Observer: tm}, 0)
	if err != nil { t.Fatal(err) }
	network.Register(1, service.Node())
	service.Start()
	defer service.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:21874")
	if err != nil { t.Fatal(err) }
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(tm.Unary))
	pb.RegisterKVServiceServer(server, &kv.GRPC{Server: service, NodeID: 1})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); if err := <-done; err != nil { t.Error(err) } })
	c, err := client.New(client.Config{Endpoints: []string{"127.0.0.1:21874"}})
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := c.Close(); err != nil { t.Error(err) } })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		state, err := service.Node().Status(ctx)
		if err != nil { t.Fatal(err) }
		if state.Role == raft.Leader { break }
		select { case <-ctx.Done(): t.Fatal(ctx.Err()); case <-time.After(10*time.Millisecond): }
	}
	ctx, parent := provider.Tracer("integration").Start(ctx, "client-operation")
	_, err = c.Put(ctx, "key", "value")
	parent.End()
	if err != nil { t.Fatal(err) }
	found := false
	for _, span := range exporter.GetSpans() {
		if span.Name != "/raftkv.v1.KVService/Put" { continue }
		if span.SpanContext.TraceID() != parent.SpanContext().TraceID() { t.Fatal("server span lost client trace") }
		events := make(map[string]bool)
		for _, event := range span.Events { events[event.Name] = true }
		if !events["raft.proposed"] || !events["raft.result_committed"] || !events["kv.result_applied"] { t.Fatalf("missing commit events: %+v", span.Events) }
		found = true
	}
	if !found { t.Fatal("missing server span") }
}
