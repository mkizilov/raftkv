package traceutil

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestUnaryPropagatesTraceAndRequestID(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { if err := provider.Shutdown(context.Background()); err != nil { t.Error(err) } }()
	ctx, parent := provider.Tracer("test").Start(context.Background(), "parent")
	defer parent.End()
	ctx = WithRequestID(ctx, "stable-id")
	err := UnaryClient(ctx, "/test/Method", nil, nil, nil, func(ctx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok { t.Fatal("outgoing metadata missing") }
		incoming := Incoming(metadata.NewIncomingContext(context.Background(), md))
		if RequestID(incoming) != "stable-id" { t.Fatalf("request ID: %q", RequestID(incoming)) }
		if trace.SpanContextFromContext(incoming).TraceID() != parent.SpanContext().TraceID() { t.Fatal("trace context was not propagated") }
		return nil
	})
	if err != nil { t.Fatal(err) }
}
