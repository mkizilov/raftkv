package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrWatchCompacted = errors.New("client: watch history compacted; re-read full state")

type WatchCompactedError struct { Revision uint64 }
func (e *WatchCompactedError) Error() string { return fmt.Sprintf("%s (through revision %d)", ErrWatchCompacted, e.Revision) }
func (e *WatchCompactedError) Unwrap() error { return ErrWatchCompacted }

type WatchEvent struct { Revision uint64; Key, Value string; Version uint64; Deleted bool }

// Watch resumes strictly after the last event successfully passed to handle.
// Persist that revision externally to resume after a process restart.
func (c *Client) Watch(ctx context.Context, prefix string, afterRevision uint64, handle func(WatchEvent) error) error {
	if handle == nil { return errors.New("client: watch callback is required") }
	index := 0
	if leader := c.leader.Load(); leader > 0 && leader <= uint64(len(c.clients)) { index = int(leader-1) }
	backoff := 20 * time.Millisecond
	for {
		if c.closed.Load() { return ErrClosed }
		if err := ctx.Err(); err != nil { return err }
		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := c.clients[index].Watch(streamCtx, &pb.WatchRequest{Prefix: prefix, AfterRevision: afterRevision})
		if err == nil {
			for {
				var event *pb.WatchEvent
				event, err = stream.Recv()
				if err != nil { break }
				if event.Revision <= afterRevision { cancel(); return errors.New("client: out-of-order watch event") }
				item := WatchEvent{Revision: event.Revision, Key: event.Key, Value: event.Value, Version: event.Version, Deleted: event.Deleted}
				if callbackErr := handle(item); callbackErr != nil { cancel(); return callbackErr }
				afterRevision = event.Revision
				c.leader.Store(uint64(index+1))
				backoff = 20 * time.Millisecond
			}
		}
		cancel()
		if c.closed.Load() { return ErrClosed }
		if ctx.Err() != nil { return ctx.Err() }
		next := (index + 1) % len(c.clients)
		if status.Code(err) == codes.OutOfRange {
			st, _ := status.FromError(err)
			for _, detail := range st.Details() { if d, ok := detail.(*pb.WatchCompactedDetail); ok { return &WatchCompactedError{Revision: d.CompactedRevision} } }
			return ErrWatchCompacted
		}
		switch status.Code(err) {
		case codes.FailedPrecondition:
			st, _ := status.FromError(err)
			found := false
			for _, detail := range st.Details() { if d, ok := detail.(*pb.NotLeaderDetail); ok { found = true; if target, ok := c.indices[d.LeaderAddress]; ok { next = target } } }
			if !found { return err }
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		default:
			if !errors.Is(err, io.EOF) { return err }
		}
		index = next
		timer := time.NewTimer(time.Duration(mathrand.Int64N(int64(backoff)+1)))
		select {
		case <-c.done: timer.Stop(); return ErrClosed
		case <-ctx.Done(): timer.Stop(); return ctx.Err()
		case <-timer.C:
		}
		if backoff < time.Second { backoff *= 2; if backoff > time.Second { backoff = time.Second } }
	}
}
