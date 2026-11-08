package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/you/raftkv/internal/transport"
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

var ErrStopped = errors.New("raft: node stopped")
var ErrNotImplemented = errors.New("raft: snapshot installation not implemented")

// Config describes a fixed cluster. LastLogIndex and LastLogTerm describe the
// local log tail for election safety; log replication is not implemented yet.
type Config struct {
	ID transport.NodeID
	Peers []transport.NodeID
	Transport transport.Transport
	Storage Storage
	ElectionMin, ElectionMax, Heartbeat time.Duration
	LastLogIndex, LastLogTerm uint64
	Seed int64
}

type Status struct {
	Term uint64
	Role Role
	CommitIndex uint64
}

type Node struct {
	config Config
	initial HardState
	events chan func(*nodeState)
	ctx context.Context
	cancel context.CancelFunc
	done chan struct{}
	start sync.Once
	workers sync.WaitGroup
}

type nodeState struct {
	node *Node
	hard HardState
	role Role
	votes map[transport.NodeID]bool
	random *rand.Rand
	timer *time.Timer
	err error
}

func NewNode(config Config) (*Node, error) {
	if config.Storage == nil || config.Transport == nil {
		return nil, errors.New("raft: storage and transport are required")
	}
	if config.ElectionMin == 0 { config.ElectionMin = 150 * time.Millisecond }
	if config.ElectionMax == 0 { config.ElectionMax = 300 * time.Millisecond }
	if config.Heartbeat == 0 { config.Heartbeat = 50 * time.Millisecond }
	if config.ElectionMin <= 0 || config.ElectionMax < config.ElectionMin || config.Heartbeat <= 0 || config.Heartbeat >= config.ElectionMin {
		return nil, errors.New("raft: invalid election or heartbeat interval")
	}
	seen := map[transport.NodeID]bool{config.ID: true}
	for _, peer := range config.Peers {
		if seen[peer] { return nil, fmt.Errorf("raft: duplicate node ID %d", peer) }
		seen[peer] = true
	}
	config.Peers = append([]transport.NodeID(nil), config.Peers...)
	hard, err := config.Storage.Load()
	if err != nil { return nil, fmt.Errorf("raft: load state: %w", err) }
	ctx, cancel := context.WithCancel(context.Background())
	return &Node{config: config, initial: hard, events: make(chan func(*nodeState), 128), ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

// Start starts the state owner once, after the handler has been registered.
func (n *Node) Start() { n.start.Do(func() { go n.run() }) }

// Stop cancels outstanding RPCs and waits for the owner and workers to exit.
func (n *Node) Stop() {
	n.Start()
	n.cancel()
	<-n.done
	n.workers.Wait()
}

func (n *Node) run() {
	seed := n.config.Seed ^ (int64(n.config.ID) * 6364136223846793005)
	s := &nodeState{node: n, hard: n.initial, role: Follower, random: rand.New(rand.NewSource(seed))}
	s.timer = time.NewTimer(s.electionTimeout())
	defer func() { s.timer.Stop(); n.cancel(); close(n.done) }()
	for {
		select {
		case <-n.ctx.Done():
			return
		case event := <-n.events:
			event(s)
			if s.err != nil { return }
		case <-s.timer.C:
			if s.role == Leader {
				s.heartbeats()
				s.reset(n.config.Heartbeat)
			} else {
				s.elect()
				if s.err != nil { return }
			}
		}
	}
}

func (s *nodeState) electionTimeout() time.Duration {
	span := s.node.config.ElectionMax - s.node.config.ElectionMin
	if span == 0 { return s.node.config.ElectionMin }
	return s.node.config.ElectionMin + time.Duration(s.random.Int63n(int64(span)))
}

func (s *nodeState) reset(d time.Duration) {
	if !s.timer.Stop() {
		select { case <-s.timer.C: default: }
	}
	s.timer.Reset(d)
}

func (s *nodeState) persist(hard HardState) bool {
	if err := s.node.config.Storage.Save(hard); err != nil {
		s.err = fmt.Errorf("raft: persist state: %w", err)
		return false
	}
	s.hard = hard
	return true
}

func (s *nodeState) observeTerm(term uint64) bool {
	if term <= s.hard.Term { return true }
	if !s.persist(HardState{Term: term}) { return false }
	s.role = Follower
	s.votes = nil
	s.reset(s.electionTimeout())
	return true
}

func (s *nodeState) elect() {
	hard := HardState{Term: s.hard.Term + 1, VotedFor: s.node.config.ID, HasVote: true}
	if !s.persist(hard) { return }
	s.role = Candidate
	s.votes = map[transport.NodeID]bool{s.node.config.ID: true}
	s.reset(s.electionTimeout())
	if len(s.votes) > (len(s.node.config.Peers)+1)/2 { s.becomeLeader(); return }
	req := transport.RequestVoteRequest{Term: hard.Term, CandidateID: s.node.config.ID, LastLogIndex: s.node.config.LastLogIndex, LastLogTerm: s.node.config.LastLogTerm}
	for _, peer := range s.node.config.Peers {
		peer := peer
		s.node.spawn(func(ctx context.Context) {
			response, err := s.node.config.Transport.RequestVote(ctx, peer, req)
			if err != nil { return }
			s.node.post(func(state *nodeState) {
				if !state.observeTerm(response.Term) { return }
				if state.role != Candidate || state.hard.Term != req.Term || response.Term != req.Term || !response.VoteGranted { return }
				state.votes[peer] = true
				if len(state.votes) > (len(state.node.config.Peers)+1)/2 { state.becomeLeader() }
			})
		})
	}
}

func (s *nodeState) becomeLeader() {
	s.role = Leader
	s.votes = nil
	s.heartbeats()
	s.reset(s.node.config.Heartbeat)
}

func (s *nodeState) heartbeats() {
	req := transport.AppendEntriesRequest{Term: s.hard.Term, LeaderID: s.node.config.ID}
	for _, peer := range s.node.config.Peers {
		peer := peer
		s.node.spawn(func(ctx context.Context) {
			response, err := s.node.config.Transport.AppendEntries(ctx, peer, req)
			if err == nil { s.node.post(func(state *nodeState) { state.observeTerm(response.Term) }) }
		})
	}
}

func (n *Node) spawn(fn func(context.Context)) {
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		ctx, cancel := context.WithTimeout(n.ctx, n.config.ElectionMin)
		defer cancel()
		fn(ctx)
	}()
}

func (n *Node) post(event func(*nodeState)) {
	select { case n.events <- event: case <-n.done: }
}

func (n *Node) submit(ctx context.Context, event func(*nodeState)) error {
	select {
	case n.events <- event: return nil
	case <-ctx.Done(): return ctx.Err()
	case <-n.done: return ErrStopped
	}
}

type result[T any] struct { value T; err error }

func call[T any](ctx context.Context, n *Node, fn func(*nodeState) (T, error)) (T, error) {
	reply := make(chan result[T], 1)
	var zero T
	if err := n.submit(ctx, func(s *nodeState) { value, err := fn(s); reply <- result[T]{value, err} }); err != nil { return zero, err }
	select {
	case r := <-reply: return r.value, r.err
	case <-ctx.Done(): return zero, ctx.Err()
	case <-n.done:
		select { case r := <-reply: return r.value, r.err; default: return zero, ErrStopped }
	}
}

func (n *Node) Status(ctx context.Context) (Status, error) {
	return call(ctx, n, func(s *nodeState) (Status, error) { return Status{Term: s.hard.Term, Role: s.role}, nil })
}

func (n *Node) RequestVote(ctx context.Context, req transport.RequestVoteRequest) (transport.RequestVoteResponse, error) {
	return call(ctx, n, func(s *nodeState) (transport.RequestVoteResponse, error) {
		if !s.observeTerm(req.Term) { return transport.RequestVoteResponse{}, s.err }
		response := transport.RequestVoteResponse{Term: s.hard.Term}
		upToDate := req.LastLogTerm > n.config.LastLogTerm || (req.LastLogTerm == n.config.LastLogTerm && req.LastLogIndex >= n.config.LastLogIndex)
		if req.Term == s.hard.Term && upToDate && (!s.hard.HasVote || s.hard.VotedFor == req.CandidateID) {
			hard := s.hard
			hard.HasVote, hard.VotedFor = true, req.CandidateID
			if !s.persist(hard) { return transport.RequestVoteResponse{}, s.err }
			s.reset(s.electionTimeout())
			response.VoteGranted = true
		}
		return response, nil
	})
}

func (n *Node) AppendEntries(ctx context.Context, req transport.AppendEntriesRequest) (transport.AppendEntriesResponse, error) {
	return call(ctx, n, func(s *nodeState) (transport.AppendEntriesResponse, error) {
		if !s.observeTerm(req.Term) { return transport.AppendEntriesResponse{}, s.err }
		response := transport.AppendEntriesResponse{Term: s.hard.Term}
		if req.Term < s.hard.Term { return response, nil }
		s.role = Follower
		s.votes = nil
		s.reset(s.electionTimeout())
		// Until replication exists, only the empty log prefix is supported.
		response.Success = req.PrevLogIndex == 0 && req.PrevLogTerm == 0 && len(req.Entries) == 0
		return response, nil
	})
}

func (n *Node) InstallSnapshot(ctx context.Context, req transport.InstallSnapshotRequest) (transport.InstallSnapshotResponse, error) {
	return call(ctx, n, func(s *nodeState) (transport.InstallSnapshotResponse, error) {
		if !s.observeTerm(req.Term) { return transport.InstallSnapshotResponse{}, s.err }
		return transport.InstallSnapshotResponse{Term: s.hard.Term}, ErrNotImplemented
	})
}
