package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/transport"
)

func watchEvent(t *testing.T, ch <-chan WatchEvent) WatchEvent {
	t.Helper()
	select { case event := <-ch: return event; case <-time.After(5*time.Second): t.Fatal("watch event timed out"); return WatchEvent{} }
}

func TestWatchResumesAfterLeaderChange(t *testing.T) {
	c := makeCluster(t, 3, 0)
	leader := c.leader(t, -1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events := make(chan WatchEvent, 8)
	ended := make(chan error, 1)
	go func() { ended <- c.servers[leader].Server.Watch(ctx, "app/", 0, func(event WatchEvent) error { events <- event; return nil }) }()
	if _, err := c.servers[leader].Put(ctx, "watch-writer", 1, "app/one", "one"); err != nil { t.Fatal(err) }
	first := watchEvent(t, events)
	if first.Key != "app/one" || first.Value != "one" || first.Version != 1 || first.Revision == 0 { t.Fatalf("first event: %+v", first) }
	c.network.Disconnect(transport.NodeID(leader+1))
	select {
	case err := <-ended:
		if !errors.Is(err, ErrWatchQuorum) && !errors.Is(err, ErrNotLeader) { t.Fatalf("old leader watch: %v", err) }
	case <-time.After(3*time.Second): t.Fatal("isolated watch did not terminate")
	}
	nextLeader := c.leader(t, leader)
	resumed := make(chan WatchEvent, 8)
	resumedDone := make(chan error, 1)
	go func() { resumedDone <- c.servers[nextLeader].Server.Watch(ctx, "app/", first.Revision, func(event WatchEvent) error { resumed <- event; return nil }) }()
	if _, err := c.servers[nextLeader].Put(ctx, "watch-writer", 2, "other/key", "ignored"); err != nil { t.Fatal(err) }
	if _, err := c.servers[nextLeader].Put(ctx, "watch-writer", 3, "app/two", "two"); err != nil { t.Fatal(err) }
	second := watchEvent(t, resumed)
	if second.Key != "app/two" || second.Value != "two" || second.Revision <= first.Revision { t.Fatalf("resumed event: %+v", second) }
	cancel()
	select { case <-resumedDone: case <-time.After(time.Second): t.Fatal("watch did not stop") }
}

func TestWatchFallsBehindHistory(t *testing.T) {
	c := makeCluster(t, 1, 0)
	leader := c.leader(t, -1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 1; i <= watchHistoryLimit+1; i++ {
		if _, err := c.servers[leader].Put(ctx, "writer", uint64(i), "prefix/key", fmt.Sprint(i)); err != nil { t.Fatal(err) }
	}
	err := c.servers[leader].Server.Watch(ctx, "prefix/", 0, func(WatchEvent) error { t.Error("delivered event before compacted error"); return nil })
	var compacted *CompactedError
	if !errors.As(err, &compacted) || compacted.Revision == 0 { t.Fatalf("compacted: %v", err) }
	stop := errors.New("received event")
	var first WatchEvent
	err = c.servers[leader].Server.Watch(ctx, "prefix/", compacted.Revision, func(event WatchEvent) error { first = event; return stop })
	if !errors.Is(err, stop) || first.Revision <= compacted.Revision { t.Fatalf("retained event: %+v %v", first, err) }
}

func TestWatchFiltersAndDeduplicatesChanges(t *testing.T) {
	c := makeCluster(t, 1, 0)
	leader := c.leader(t, -1)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	s := c.servers[leader]
	if _, err := s.Put(ctx, "writer", 1, "app/key", "one"); err != nil { t.Fatal(err) }
	if _, err := s.Put(ctx, "writer", 1, "app/key", "one"); err != nil { t.Fatal(err) }
	if result, err := s.CompareAndSwap(ctx, "writer", 2, "app/key", 0, "bad"); err != nil || result.Swapped { t.Fatalf("failed CAS: %+v %v", result, err) }
	if _, err := s.Put(ctx, "writer", 3, "other/key", "ignored"); err != nil { t.Fatal(err) }
	if _, err := s.Delete(ctx, "writer", 4, "app/key"); err != nil { t.Fatal(err) }
	stop := errors.New("received two events")
	var events []WatchEvent
	err := s.Server.Watch(ctx, "app/", 0, func(event WatchEvent) error {
		events = append(events, event)
		if len(events) == 2 { return stop }
		return nil
	})
	if !errors.Is(err, stop) || len(events) != 2 || events[0].Value != "one" || events[0].Version != 1 || !events[1].Deleted || events[1].Version != 2 || events[1].Revision <= events[0].Revision { t.Fatalf("events: %+v %v", events, err) }
}

func TestSlowWatcherFallsBehind(t *testing.T) {
	c := makeCluster(t, 1, 0)
	leader := c.leader(t, -1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	blocked, release := make(chan struct{}, 1), make(chan struct{})
	released := false
	defer func() { if !released { close(release) } }()
	done := make(chan error, 1)
	go func() {
		done <- c.servers[leader].Server.Watch(ctx, "app/", 0, func(WatchEvent) error { blocked <- struct{}{}; <-release; return nil })
	}()
	if _, err := c.servers[leader].Put(ctx, "writer", 1, "app/key", "first"); err != nil { t.Fatal(err) }
	select { case <-blocked: case <-time.After(3*time.Second): t.Fatal("watch did not receive first event") }
	for i := 2; i <= watchHistoryLimit+2; i++ {
		if _, err := c.servers[leader].Put(ctx, "writer", uint64(i), "app/key", fmt.Sprint(i)); err != nil { t.Fatal(err) }
	}
	close(release)
	released = true
	select {
	case err := <-done:
		if !errors.Is(err, ErrWatchCompacted) { t.Fatalf("slow watcher: %v", err) }
	case <-time.After(3*time.Second): t.Fatal("slow watcher did not report compaction")
	}
}

func TestWatchHistorySurvivesSnapshotRestart(t *testing.T) {
	c := makeCluster(t, 1, 1)
	leader := c.leader(t, -1)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := c.servers[leader].Put(ctx, "writer", 1, "app/key", "value"); err != nil { t.Fatal(err) }
	deadline := time.Now().Add(3*time.Second)
	for {
		snapshot, err := c.stores[leader].LoadSnapshot()
		if err != nil { t.Fatal(err) }
		if snapshot.Index > 0 { break }
		if time.Now().After(deadline) { t.Fatal("snapshot not created") }
		time.Sleep(time.Millisecond)
	}
	c.servers[leader].Close()
	id := transport.NodeID(leader+1)
	restarted, err := New(raft.Config{ID: id, Transport: c.network.Endpoint(id), Storage: c.stores[leader]}, 1)
	if err != nil { t.Fatal(err) }
	c.servers[leader] = &recordedServer{Server: restarted, history: c.history}
	c.network.Register(id, restarted.Node())
	restarted.Start()
	c.leader(t, -1)
	stop := errors.New("received event")
	var event WatchEvent
	err = restarted.Watch(ctx, "app/", 0, func(e WatchEvent) error { event = e; return stop })
	if !errors.Is(err, stop) || event.Key != "app/key" || event.Value != "value" { t.Fatalf("restored event: %+v %v", event, err) }
}

func TestLegacySnapshotRequiresReread(t *testing.T) {
	legacy, err := json.Marshal(image{Format: 1, Index: 10, Values: map[string]Result{}, Requests: map[string]map[uint64]record{}})
	if err != nil { t.Fatal(err) }
	s := &Server{state: image{Format: 1, HistoryFormat: 1, Values: map[string]Result{}, Requests: map[string]map[uint64]record{}}, applied: make(chan struct{}), pending: make(map[requestID]map[*waiter]struct{})}
	if err := s.applyMessage(raft.ApplyMsg{SnapshotValid: true, SnapshotIndex: 10, Snapshot: legacy}); err != nil { t.Fatal(err) }
	result := s.nextWatch("", 0)
	var compacted *CompactedError
	if !errors.As(result.err, &compacted) || compacted.Revision != 10 { t.Fatalf("legacy history: %+v", result) }
}

func TestOversizedWatchEventCompactsHistory(t *testing.T) {
	s := &Server{state: image{HistoryFormat: 1}}
	s.recordWatch(WatchEvent{Revision: 1, Key: "k", Value: "old", Version: 1})
	s.recordWatch(WatchEvent{Revision: 2, Key: "k", Value: strings.Repeat("x", watchHistoryByteLimit), Version: 2})
	if len(s.state.Events) != 0 || s.state.CompactedRevision != 2 { t.Fatalf("history was not bounded: %+v", s.state) }
	if !errors.Is(s.nextWatch("", 1).err, ErrWatchCompacted) { t.Fatal("oversized event did not report history loss") }
}
