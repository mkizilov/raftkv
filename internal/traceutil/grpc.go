// Package traceutil propagates trace context and request IDs over gRPC.
package traceutil

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type contextKey struct{}
var nextID atomic.Uint64

func WithRequestID(ctx context.Context, id string) context.Context { return context.WithValue(ctx, contextKey{}, id) }
func RequestID(ctx context.Context) string {
	if id, ok := ctx.Value(contextKey{}).(string); ok && id != "" { return id }
	return "-"
}

type carrier struct { md metadata.MD }
func (c carrier) Get(key string) string { values := c.md.Get(key); if len(values) > 0 { return values[0] }; return "" }
func (c carrier) Set(key, value string) { c.md.Set(key, value) }
func (c carrier) Keys() []string { keys := make([]string, 0, len(c.md)); for key := range c.md { keys = append(keys, key) }; return keys }

func Incoming(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	ctx = propagation.TraceContext{}.Extract(ctx, carrier{md: md})
	ids := md.Get("x-request-id")
	id := ""
	if len(ids) > 0 && len(ids[0]) <= 128 { id = ids[0] }
	if id == "" { id = fmt.Sprintf("server-%d", nextID.Add(1)) }
	return WithRequestID(ctx, id)
}

func outgoing(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if md == nil { md = metadata.MD{} }
	propagation.TraceContext{}.Inject(ctx, carrier{md: md})
	id := RequestID(ctx)
	if id == "-" { id = fmt.Sprintf("client-%d", nextID.Add(1)) }
	md.Set("x-request-id", id)
	return metadata.NewOutgoingContext(ctx, md)
}

func UnaryClient(ctx context.Context, method string, req, reply interface{}, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, span := otel.Tracer("raftkv/client").Start(ctx, method, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("rpc.method", method)))
	defer span.End()
	err := invoke(outgoing(ctx), method, req, reply, conn, opts...)
	if err != nil { span.RecordError(err); span.SetStatus(codes.Error, err.Error()) }
	return err
}

type clientStream struct { grpc.ClientStream; span trace.Span; once sync.Once }
func (s *clientStream) RecvMsg(m interface{}) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil { s.once.Do(func() { s.span.End() }) }
	return err
}

func StreamClient(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, span := otel.Tracer("raftkv/client").Start(ctx, method, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("rpc.method", method)))
	stream, err := streamer(outgoing(ctx), desc, conn, method, opts...)
	if err != nil { span.RecordError(err); span.SetStatus(codes.Error, err.Error()); span.End(); return nil, err }
	wrapped := &clientStream{ClientStream: stream, span: span}
	context.AfterFunc(stream.Context(), func() { wrapped.once.Do(func() { wrapped.span.End() }) })
	return wrapped, nil
}
