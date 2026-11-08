package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"strconv"

	"github.com/you/raftkv/internal/raft"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrNotLeader = errors.New("kv: not leader")
	ErrStopped = errors.New("kv: server stopped")
	ErrRequestConflict = errors.New("kv: client sequence reused for a different request")
	ErrVersionExhausted = errors.New("kv: key version exhausted")
	ErrInvalidRequest = errors.New("kv: client ID, positive sequence and key are required")
	ErrInvalidKey = errors.New("kv: key is required")
)

// Result describes the value and version after an operation. Deleted keys
// retain their version; an untouched key has version zero and Found is false.
type Result struct {
	Value string
	Version uint64
	Found bool
	Swapped bool
}

type command struct {
	ClientID string
	Sequence uint64
	Operation string
	Key string
	Value string
	Expected uint64
}

type record struct {
	Command command
	Result Result
	Error string
}

type image struct {
	Format int
	Index uint64
	Values map[string]Result
	Requests map[string]map[uint64]record
	HistoryFormat int
	CompactedRevision uint64
	Events []WatchEvent
}

type requestID struct { client string; sequence uint64 }
type response struct { result Result; err error }
type waiter struct { command command; ch chan response }
type readQuery struct { key string; reply chan readState }
type readState struct { index uint64; result Result }

// Server applies committed commands in order. Register Node as the transport
// handler before calling Start. A zero snapshot threshold disables snapshots;
// a positive threshold counts retained command bytes plus Raft term bytes.
type Server struct {
	node *raft.Node
	apply chan raft.ApplyMsg
	threshold uint64
	ctx context.Context
	cancel context.CancelFunc
	done chan struct{}
	start sync.Once
	mu sync.Mutex
	pending map[requestID]map[*waiter]struct{}
	reads chan readQuery
	watches chan watchQuery
	applied chan struct{}
	err error
	state image // owned exclusively by the apply goroutine
	observer raft.Observer
}

func New(config raft.Config, snapshotThreshold uint64) (*Server, error) {
	if config.ApplyCh != nil { return nil, errors.New("kv: Raft apply channel is owned by the server") }
	apply := make(chan raft.ApplyMsg)
	config.ApplyCh = apply
	node, err := raft.NewNode(config)
	if err != nil { return nil, err }
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{node: node, apply: apply, threshold: snapshotThreshold, ctx: ctx, cancel: cancel, done: make(chan struct{}), reads: make(chan readQuery), watches: make(chan watchQuery), applied: make(chan struct{}), pending: make(map[requestID]map[*waiter]struct{}), observer: config.Observer, state: image{Format: 1, HistoryFormat: 1, Values: make(map[string]Result), Requests: make(map[string]map[uint64]record)}}, nil
}

func (s *Server) Node() *raft.Node { return s.node }

func (s *Server) Healthy() bool {
	select { case <-s.done: return false; case <-s.node.Done(): return false; case <-s.ctx.Done(): return false; default: return true }
}

func (s *Server) Start() {
	s.start.Do(func() { go s.run(); s.node.Run() })
}

func (s *Server) Close() {
	s.Start()
	s.cancel()
	s.node.Stop()
	<-s.done
}

func (s *Server) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil { return s.err }
	return ErrStopped
}

func (s *Server) run() {
	defer close(s.done)
	defer s.node.Stop()
	for {
		select {
		case <-s.ctx.Done(): return
		case <-s.node.Done(): return
		case query := <-s.reads:
			query.reply <- readState{index: s.state.Index, result: s.state.Values[query.key]}
		case query := <-s.watches:
			if s.state.Index < query.barrier { query.reply <- watchResult{scanned: query.after} } else { query.reply <- s.nextWatch(query.prefix, query.after) }
		case msg := <-s.apply:
			if err := s.applyMessage(msg); err != nil {
				s.mu.Lock()
				s.err = err
				s.mu.Unlock()
				return
			}
		}
	}
}

func (s *Server) Get(ctx context.Context, client string, sequence uint64, key string) (Result, error) {
	return s.execute(ctx, command{ClientID: client, Sequence: sequence, Operation: "get", Key: key})
}

// ReadLatest is a linearizable read without a client request identity. Unlike
// Get, retries are independent reads and do not return a cached prior result.
func (s *Server) ReadLatest(ctx context.Context, key string) (Result, error) {
	if key == "" { return Result{}, ErrInvalidKey }
	index, _, err := s.node.ReadIndex(ctx)
	if errors.Is(err, raft.ErrNotLeader) { return Result{}, ErrNotLeader }
	if err != nil { return Result{}, err }
	for {
		s.mu.Lock()
		changed := s.applied
		s.mu.Unlock()
		reply := make(chan readState, 1)
		select {
		case s.reads <- readQuery{key: key, reply: reply}:
		case <-ctx.Done(): return Result{}, ctx.Err()
		case <-s.done: return Result{}, s.failure()
		}
		select {
		case state := <-reply:
			if state.index >= index { return state.result, nil }
		case <-ctx.Done(): return Result{}, ctx.Err()
		case <-s.done: return Result{}, s.failure()
		}
		select {
		case <-changed:
		case <-ctx.Done(): return Result{}, ctx.Err()
		case <-s.done: return Result{}, s.failure()
		}
	}
}

func (s *Server) advanceApplied() {
	if s.observer != nil { s.observer.Applied(s.state.Index) }
	s.mu.Lock()
	close(s.applied)
	s.applied = make(chan struct{})
	s.mu.Unlock()
}

func (s *Server) Put(ctx context.Context, client string, sequence uint64, key, value string) (Result, error) {
	return s.execute(ctx, command{ClientID: client, Sequence: sequence, Operation: "put", Key: key, Value: value})
}

func (s *Server) Delete(ctx context.Context, client string, sequence uint64, key string) (Result, error) {
	return s.execute(ctx, command{ClientID: client, Sequence: sequence, Operation: "delete", Key: key})
}

func (s *Server) CompareAndSwap(ctx context.Context, client string, sequence uint64, key string, expectedVersion uint64, value string) (Result, error) {
	return s.execute(ctx, command{ClientID: client, Sequence: sequence, Operation: "cas", Key: key, Expected: expectedVersion, Value: value})
}

func (s *Server) execute(ctx context.Context, cmd command) (Result, error) {
	if cmd.ClientID == "" || cmd.Sequence == 0 || cmd.Key == "" { return Result{}, ErrInvalidRequest }
	payload, err := json.Marshal(cmd)
	if err != nil { return Result{}, err }
	id := requestID{cmd.ClientID, cmd.Sequence}
	w := &waiter{command: cmd, ch: make(chan response, 1)}
	s.mu.Lock()
	if s.pending[id] == nil { s.pending[id] = make(map[*waiter]struct{}) }
	s.pending[id][w] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending[id], w)
		if len(s.pending[id]) == 0 { delete(s.pending, id) }
		s.mu.Unlock()
	}()
	index, term, leader, err := s.node.Propose(ctx, payload)
	span := trace.SpanFromContext(ctx)
	if err == nil && leader { span.AddEvent("raft.proposed", trace.WithAttributes(attribute.String("raft.index", strconv.FormatUint(index, 10)), attribute.String("raft.term", strconv.FormatUint(term, 10)))) }
	if err != nil { return Result{}, err }
	if !leader { return Result{}, ErrNotLeader }
	select {
	case r := <-w.ch: span.AddEvent("raft.result_committed"); span.AddEvent("kv.result_applied"); return r.result, r.err
	case <-ctx.Done(): return Result{}, ctx.Err()
	case <-s.done: return Result{}, s.failure()
	}
}

func recordResponse(r record, cmd command) response {
	if r.Command != cmd { return response{err: ErrRequestConflict} }
	if r.Error == "version_exhausted" { return response{err: ErrVersionExhausted} }
	return response{result: r.Result}
}

func (s *Server) notify(id requestID, r record) {
	s.mu.Lock()
	waiters := make([]*waiter, 0, len(s.pending[id]))
	for w := range s.pending[id] { waiters = append(waiters, w) }
	s.mu.Unlock()
	for _, w := range waiters {
		select { case w.ch <- recordResponse(r, w.command): default: }
	}
}

func (s *Server) applyMessage(msg raft.ApplyMsg) error {
	if msg.SnapshotValid {
		var restored image
		if err := json.Unmarshal(msg.Snapshot, &restored); err != nil { return fmt.Errorf("kv: invalid snapshot: %w", err) }
		if restored.Format != 1 || restored.Index != msg.SnapshotIndex || restored.Values == nil || restored.Requests == nil { return errors.New("kv: invalid snapshot state") }
		if restored.Index <= s.state.Index { return nil }
		if restored.HistoryFormat == 0 { restored.HistoryFormat, restored.CompactedRevision = 1, restored.Index }
		if restored.HistoryFormat != 1 || len(restored.Events) > watchHistoryLimit || restored.CompactedRevision > restored.Index { return errors.New("kv: invalid snapshot event history") }
		lastRevision := restored.CompactedRevision
		historyBytes := 0
		for _, event := range restored.Events {
			if event.Revision <= lastRevision || event.Revision > restored.Index || event.Key == "" || event.Version == 0 || (event.Deleted && event.Value != "") { return errors.New("kv: invalid snapshot watch event") }
			historyBytes += watchEventSize(event)
			if historyBytes > watchHistoryByteLimit { return errors.New("kv: snapshot watch history exceeds byte limit") }
			lastRevision = event.Revision
		}
		s.state = restored
		if s.observer != nil { s.observer.SnapshotSize(uint64(len(msg.Snapshot))) }
		s.advanceApplied()
		for client, requests := range s.state.Requests {
			for sequence, r := range requests { s.notify(requestID{client, sequence}, r) }
		}
		return nil
	}
	if msg.Index != s.state.Index+1 { return fmt.Errorf("kv: applied index %d after %d", msg.Index, s.state.Index) }
	var cmd command
	if err := json.Unmarshal(msg.Command, &cmd); err != nil { return fmt.Errorf("kv: invalid command: %w", err) }
	if cmd.Operation == "watch_barrier" {
		s.state.Index = msg.Index
		s.advanceApplied()
		return s.snapshotIfNeeded(msg.Index)
	}
	if cmd.ClientID == "" || cmd.Sequence == 0 || cmd.Key == "" { return errors.New("kv: invalid committed command") }
	if cmd.Operation != "get" && cmd.Operation != "put" && cmd.Operation != "delete" && cmd.Operation != "cas" { return errors.New("kv: unknown committed operation") }
	requests := s.state.Requests[cmd.ClientID]
	if requests == nil { requests = make(map[uint64]record); s.state.Requests[cmd.ClientID] = requests }
	r, exists := requests[cmd.Sequence]
	if !exists {
		r = record{Command: cmd}
		current := s.state.Values[cmd.Key]
		write := cmd.Operation == "put" || cmd.Operation == "delete" || (cmd.Operation == "cas" && current.Version == cmd.Expected)
		if write && current.Version == ^uint64(0) {
			r.Error = "version_exhausted"
		} else {
			if write {
				current.Version++
				current.Value, current.Found = cmd.Value, cmd.Operation != "delete"
				if !current.Found { current.Value = "" }
				s.state.Values[cmd.Key] = current
				s.recordWatch(WatchEvent{Revision: msg.Index, Key: cmd.Key, Value: current.Value, Version: current.Version, Deleted: !current.Found})
			}
			r.Result = current
			r.Result.Swapped = cmd.Operation == "cas" && write
		}
		requests[cmd.Sequence] = r
	}
	s.state.Index = msg.Index
	s.advanceApplied()
	s.notify(requestID{cmd.ClientID, cmd.Sequence}, r)
	return s.snapshotIfNeeded(msg.Index)
}

func (s *Server) snapshotIfNeeded(index uint64) error {
	if s.threshold == 0 { return nil }
	status, err := s.node.Status(s.ctx)
	if err != nil { return err }
	if status.LogBytes < s.threshold { return nil }
	data, err := json.Marshal(s.state)
	if err != nil { return fmt.Errorf("kv: encode snapshot: %w", err) }
	if err = s.node.Snapshot(index, data); err != nil {
		if !errors.Is(err, raft.ErrSnapshotOutdated) { return fmt.Errorf("kv: create snapshot: %w", err) }
	} else if s.observer != nil { s.observer.SnapshotSize(uint64(len(data))) }
	return nil
}
