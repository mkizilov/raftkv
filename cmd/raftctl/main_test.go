package main

import (
	"context"
	"io"
	"testing"
)

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{}, {"get"}, {"put", "key"}, {"cas", "key", "bad", "value"}, {"bench", "0", "key"}, {"-client-id", "client", "get", "key"}, {"unknown"}} {
		if err := run(context.Background(), args, io.Discard, io.Discard); err == nil { t.Fatalf("accepted %v", args) }
	}
}
