package kv

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRequestSpanIncludesCommitEvents(t *testing.T) {
	c := makeCluster(t, 1, 0)
	leader := c.leader(t, -1)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	ctx, cancel := deadline(t)
	defer cancel()
	ctx, span := provider.Tracer("test").Start(ctx, "put")
	_, err := c.servers[leader].Put(ctx, "tracing-client", 1, "key", "value")
	span.End()
	if err != nil { t.Fatal(err) }
	spans := exporter.GetSpans()
	if len(spans) != 1 { t.Fatalf("spans = %d", len(spans)) }
	seen := make(map[string]bool)
	for _, event := range spans[0].Events { seen[event.Name] = true }
	if !seen["raft.proposed"] || !seen["raft.result_committed"] || !seen["kv.result_applied"] { t.Fatalf("missing commit trace events: %+v", spans[0].Events) }
}
