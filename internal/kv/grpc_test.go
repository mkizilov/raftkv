package kv

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadLatestInvalidKeyIsClientError(t *testing.T) {
	server := &Server{}
	_, err := server.ReadLatest(context.Background(), "")
	if !errors.Is(err, ErrInvalidKey) { t.Fatalf("invalid key: %v", err) }
	gateway := &GRPC{}
	_, err = gateway.response(context.Background(), Result{}, err)
	if status.Code(err) != codes.InvalidArgument { t.Fatalf("gRPC status = %v", err) }
}
