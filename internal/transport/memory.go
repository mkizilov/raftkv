package transport

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("transport: message unavailable")

// Network simulates independent request and response loss and delay. A seeded
// RNG controls decisions; concurrent caller scheduling remains nondeterministic.
type Network struct {
	mu       sync.Mutex
	random   *rand.Rand
	handlers map[NodeID]Handler
	enabled  map[NodeID]bool
	groups   map[NodeID]int
	drop     float64
	delay    time.Duration
	epoch    uint64
}

func NewNetwork(seed int64) *Network {
	return &Network{random: rand.New(rand.NewSource(seed)), handlers: make(map[NodeID]Handler), enabled: make(map[NodeID]bool)}
}

// Register adds or replaces a handler and enables the node.
func (n *Network) Register(id NodeID, handler Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = handler
	n.enabled[id] = true
	n.epoch++
}

func (n *Network) Endpoint(id NodeID) Transport { return endpoint{network: n, from: id} }

func (n *Network) Disconnect(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.enabled[id] = false
	n.epoch++
}

func (n *Network) Reconnect(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.enabled[id] = true
	n.epoch++
}

// Partition isolates groups from each other. Nodes not listed are isolated
// from every other node. An empty argument heals the partition.
func (n *Network) Partition(groups ...[]NodeID) error {
	partition := make(map[NodeID]int)
	for i, group := range groups {
		for _, id := range group {
			if _, exists := partition[id]; exists {
				return fmt.Errorf("transport: node %d appears in multiple groups", id)
			}
			partition[id] = i
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(groups) == 0 {
		n.groups = nil
	} else {
		n.groups = partition
	}
	n.epoch++
	return nil
}

// Configure sets the probability of dropping each message and the maximum
// independently sampled delivery delay. Delays can reorder concurrent RPCs.
func (n *Network) Configure(drop float64, maxDelay time.Duration) error {
	if !(drop >= 0 && drop <= 1) || maxDelay < 0 {
		return errors.New("transport: invalid loss or delay")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop, n.delay = drop, maxDelay
	return nil
}

func (n *Network) connected(from, to NodeID) bool {
	if !n.enabled[from] || !n.enabled[to] || n.handlers[from] == nil || n.handlers[to] == nil {
		return false
	}
	if n.groups == nil || from == to {
		return true
	}
	a, okA := n.groups[from]
	b, okB := n.groups[to]
	return okA && okB && a == b
}

// deliver never holds the network mutex during a wait or handler invocation.
func (n *Network) deliver(ctx context.Context, from, to NodeID, epoch uint64) (Handler, uint64, error) {
	n.mu.Lock()
	if (epoch != 0 && epoch != n.epoch) || !n.connected(from, to) || n.random.Float64() < n.drop {
		n.mu.Unlock()
		return nil, 0, ErrUnavailable
	}
	current := n.epoch
	handler := n.handlers[to]
	var delay time.Duration
	if n.delay > 0 {
		delay = time.Duration(n.random.Int63n(int64(n.delay)))
	}
	n.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	n.mu.Lock()
	valid := current == n.epoch && n.connected(from, to)
	n.mu.Unlock()
	if !valid {
		return nil, 0, ErrUnavailable
	}
	return handler, current, nil
}

type endpoint struct {
	network *Network
	from    NodeID
}

func (e endpoint) RequestVote(ctx context.Context, to NodeID, request RequestVoteRequest) (RequestVoteResponse, error) {
	h, epoch, err := e.network.deliver(ctx, e.from, to, 0)
	if err != nil { return RequestVoteResponse{}, err }
	response, err := h.RequestVote(ctx, request)
	if err != nil { return RequestVoteResponse{}, err }
	_, _, err = e.network.deliver(ctx, to, e.from, epoch)
	if err != nil { return RequestVoteResponse{}, err }
	return response, nil
}

func (e endpoint) AppendEntries(ctx context.Context, to NodeID, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	entries := make([]LogEntry, len(request.Entries))
	for i, entry := range request.Entries {
		entries[i] = LogEntry{Term: entry.Term, Command: append([]byte(nil), entry.Command...)}
	}
	request.Entries = entries
	h, epoch, err := e.network.deliver(ctx, e.from, to, 0)
	if err != nil { return AppendEntriesResponse{}, err }
	response, err := h.AppendEntries(ctx, request)
	if err != nil { return AppendEntriesResponse{}, err }
	_, _, err = e.network.deliver(ctx, to, e.from, epoch)
	if err != nil { return AppendEntriesResponse{}, err }
	return response, nil
}

func (e endpoint) InstallSnapshot(ctx context.Context, to NodeID, request InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	request.Data = append([]byte(nil), request.Data...)
	h, epoch, err := e.network.deliver(ctx, e.from, to, 0)
	if err != nil { return InstallSnapshotResponse{}, err }
	response, err := h.InstallSnapshot(ctx, request)
	if err != nil { return InstallSnapshotResponse{}, err }
	_, _, err = e.network.deliver(ctx, to, e.from, epoch)
	if err != nil { return InstallSnapshotResponse{}, err }
	return response, nil
}
