// Package observability implements per-node logging, metrics and gRPC tracing.
package observability

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/traceutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type logState struct { term uint64; role string }

type Telemetry struct {
	nodeID uint64
	Logger *slog.Logger
	Registry *prometheus.Registry
	provider trace.TracerProvider
	state atomic.Pointer[logState]
	mu sync.Mutex
	lastLeader uint64
	lastLeaderTerm uint64
	seenLeader bool
	term, commit, applied, snapshot prometheus.Gauge
	role *prometheus.GaugeVec
	leaderChanges prometheus.Counter
	requests *prometheus.CounterVec
	latency *prometheus.HistogramVec
}

type logHandler struct { inner slog.Handler; telemetry *Telemetry }
func (h logHandler) Enabled(ctx context.Context, level slog.Level) bool { return h.inner.Enabled(ctx, level) }
func (h logHandler) Handle(ctx context.Context, record slog.Record) error {
	state := h.telemetry.state.Load()
	if state == nil { state = &logState{role: "follower"} }
	record.AddAttrs(slog.Uint64("node_id", h.telemetry.nodeID), slog.Uint64("term", state.term), slog.String("role", state.role), slog.String("request_id", traceutil.RequestID(ctx)))
	if span := trace.SpanContextFromContext(ctx); span.IsValid() { record.AddAttrs(slog.String("trace_id", span.TraceID().String())) }
	return h.inner.Handle(ctx, record)
}
func (h logHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return logHandler{inner: h.inner.WithAttrs(attrs), telemetry: h.telemetry} }
func (h logHandler) WithGroup(name string) slog.Handler { return logHandler{inner: h.inner.WithGroup(name), telemetry: h.telemetry} }

func New(nodeID uint64, level slog.Level, output io.Writer, provider trace.TracerProvider) *Telemetry {
	labels := prometheus.Labels{"node_id": strconv.FormatUint(nodeID, 10)}
	t := &Telemetry{nodeID: nodeID, Registry: prometheus.NewRegistry(), provider: provider}
	if t.provider == nil { t.provider = sdktrace.NewTracerProvider() }
	t.state.Store(&logState{role: "follower"})
	t.Logger = slog.New(logHandler{inner: slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level}), telemetry: t})
	t.term = prometheus.NewGauge(prometheus.GaugeOpts{Name: "raftkv_current_term", Help: "Current Raft term.", ConstLabels: labels})
	t.commit = prometheus.NewGauge(prometheus.GaugeOpts{Name: "raftkv_commit_index", Help: "Committed Raft index.", ConstLabels: labels})
	t.applied = prometheus.NewGauge(prometheus.GaugeOpts{Name: "raftkv_applied_index", Help: "State machine applied index.", ConstLabels: labels})
	t.snapshot = prometheus.NewGauge(prometheus.GaugeOpts{Name: "raftkv_snapshot_size_bytes", Help: "Most recent applied snapshot size.", ConstLabels: labels})
	t.role = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "raftkv_role", Help: "One for the current Raft role.", ConstLabels: labels}, []string{"role"})
	t.leaderChanges = prometheus.NewCounter(prometheus.CounterOpts{Name: "raftkv_leader_changes_total", Help: "Distinct observed leader and term pairs.", ConstLabels: labels})
	t.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "raftkv_rpc_requests_total", Help: "Completed gRPC requests by method and status.", ConstLabels: labels}, []string{"method", "status"})
	t.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "raftkv_rpc_duration_seconds", Help: "gRPC request duration in seconds.", ConstLabels: labels, Buckets: prometheus.DefBuckets}, []string{"method", "status"})
	t.Registry.MustRegister(t.term, t.commit, t.applied, t.snapshot, t.role, t.leaderChanges, t.requests, t.latency)
	for _, role := range []string{"follower", "candidate", "leader"} { t.role.WithLabelValues(role).Set(0) }
	t.role.WithLabelValues("follower").Set(1)
	return t
}

func roleName(role raft.Role) string {
	switch role { case raft.Candidate: return "candidate"; case raft.Leader: return "leader"; default: return "follower" }
}

func (t *Telemetry) RaftState(state raft.Status) {
	role := roleName(state.Role)
	t.state.Store(&logState{term: state.Term, role: role})
	t.term.Set(float64(state.Term))
	t.commit.Set(float64(state.CommitIndex))
	for _, name := range []string{"follower", "candidate", "leader"} { value := 0.0; if name == role { value = 1 }; t.role.WithLabelValues(name).Set(value) }
	t.mu.Lock()
	if state.LeaderKnown && (!t.seenLeader || t.lastLeader != uint64(state.LeaderID) || t.lastLeaderTerm != state.Term) {
		t.leaderChanges.Inc()
		t.seenLeader, t.lastLeader, t.lastLeaderTerm = true, uint64(state.LeaderID), state.Term
	}
	t.mu.Unlock()
}

func (t *Telemetry) Applied(index uint64) { t.applied.Set(float64(index)) }
func (t *Telemetry) SnapshotSize(size uint64) { t.snapshot.Set(float64(size)) }

// NewTracerProvider uses OTLP/HTTP when an endpoint is supplied. An empty
// endpoint records no exported spans. The caller must shut the provider down.
func NewTracerProvider(ctx context.Context, endpoint string) (*sdktrace.TracerProvider, error) {
	if endpoint == "" { return sdktrace.NewTracerProvider(), nil }
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil { return nil, errors.New("invalid OTLP endpoint") }
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithHeaders(map[string]string{}), otlptracehttp.WithTimeout(3*time.Second))
	if err != nil { return nil, err }
	return sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter)), nil
}

func (t *Telemetry) begin(ctx context.Context, method string) (context.Context, trace.Span, time.Time) {
	ctx = traceutil.Incoming(ctx)
	ctx, span := t.provider.Tracer("raftkv/server").Start(ctx, method, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("rpc.method", method), attribute.String("raft.node_id", strconv.FormatUint(t.nodeID, 10))))
	return ctx, span, time.Now()
}

func (t *Telemetry) finish(ctx context.Context, span trace.Span, start time.Time, method string, err error) {
	defer span.End()
	code := status.Code(err).String()
	elapsed := time.Since(start)
	t.requests.WithLabelValues(method, code).Inc()
	t.latency.WithLabelValues(method, code).Observe(elapsed.Seconds())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.Logger.WarnContext(ctx, "rpc completed", "method", method, "status", code, "duration_ms", elapsed.Seconds()*1000)
	} else { t.Logger.InfoContext(ctx, "rpc completed", "method", method, "status", code, "duration_ms", elapsed.Seconds()*1000) }
}

func (t *Telemetry) Unary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx, span, start := t.begin(ctx, info.FullMethod)
	response, err := handler(ctx, req)
	t.finish(ctx, span, start, info.FullMethod, err)
	return response, err
}

type serverStream struct { grpc.ServerStream; ctx context.Context }
func (s serverStream) Context() context.Context { return s.ctx }

func (t *Telemetry) Stream(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, span, start := t.begin(stream.Context(), info.FullMethod)
	err := handler(srv, serverStream{ServerStream: stream, ctx: ctx})
	t.finish(ctx, span, start, info.FullMethod, err)
	return err
}

// Handler exposes metrics, liveness and quorum-confirmed readiness.
func (t *Telemetry) Handler(live func() bool, ready func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(t.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !live() { http.Error(w, "unhealthy", http.StatusServiceUnavailable); return }
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if !live() || ready(ctx) != nil { http.Error(w, "not ready", http.StatusServiceUnavailable); return }
		_, _ = io.WriteString(w, "ready\n")
	})
	return mux
}
