// Package client provides retrying, exactly-once-aware access to RaftKV.
package client

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"net"
	"sync/atomic"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/internal/traceutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Config struct {
	Endpoints []string
	ClientID string
	InitialSequence uint64
	RPCTimeout time.Duration
}

type RequestID struct { ClientID string; Sequence uint64 }

var ErrClosed = errors.New("client: closed")

type Result struct { Value string; Version uint64; Found, Swapped bool }

type NodeStatus struct {
	Address string
	NodeID uint64
	Role string
	Term, CommitIndex uint64
	LeaderKnown bool
	LeaderID uint64
	LeaderAddress string
	VoterIDs []uint64
	Err error
}

type Client struct {
	id string
	sequence atomic.Uint64
	leader atomic.Uint64
	addresses []string
	indices map[string]int
	connections []*grpc.ClientConn
	clients []pb.KVServiceClient
	timeout time.Duration
	closed atomic.Bool
	done chan struct{}
}

// New creates one reusable connection per endpoint. An empty ClientID is
// replaced with a random ID. Reused IDs require a persisted sequence counter.
func New(config Config) (*Client, error) {
	if len(config.Endpoints) == 0 { return nil, errors.New("client: at least one endpoint is required") }
	if config.RPCTimeout == 0 { config.RPCTimeout = 500 * time.Millisecond }
	if config.RPCTimeout < 0 { return nil, errors.New("client: RPC timeout must be positive") }
	if config.ClientID == "" {
		var id [16]byte
		if _, err := cryptorand.Read(id[:]); err != nil { return nil, err }
		config.ClientID = hex.EncodeToString(id[:])
	}
	c := &Client{id: config.ClientID, indices: make(map[string]int), timeout: config.RPCTimeout, done: make(chan struct{})}
	c.sequence.Store(config.InitialSequence)
	for _, address := range config.Endpoints {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" || port == "" { _ = c.Close(); return nil, fmt.Errorf("client: invalid endpoint %q", address) }
		if _, exists := c.indices[address]; exists { _ = c.Close(); return nil, fmt.Errorf("client: duplicate endpoint %q", address) }
		conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(traceutil.UnaryClient), grpc.WithChainStreamInterceptor(traceutil.StreamClient), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
		if err != nil { _ = c.Close(); return nil, err }
		c.indices[address] = len(c.addresses)
		c.addresses = append(c.addresses, address)
		c.connections = append(c.connections, conn)
		c.clients = append(c.clients, pb.NewKVServiceClient(conn))
	}
	return c, nil
}

func requestContext(ctx context.Context, id RequestID) context.Context {
	digest := sha256.Sum256([]byte(id.ClientID))
	return traceutil.WithRequestID(ctx, fmt.Sprintf("%x-%d", digest[:8], id.Sequence))
}

func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) { return nil }
	close(c.done)
	var result error
	for _, conn := range c.connections { result = errors.Join(result, conn.Close()) }
	return result
}

// NewRequest reserves an identity that can be retained across ambiguous
// failures and reused with the same operation and arguments.
func (c *Client) NewRequest() (RequestID, error) {
	for {
		if c.closed.Load() { return RequestID{}, ErrClosed }
		old := c.sequence.Load()
		if old == ^uint64(0) { return RequestID{}, errors.New("client: sequence exhausted") }
		if c.sequence.CompareAndSwap(old, old+1) { return RequestID{ClientID: c.id, Sequence: old+1}, nil }
	}
}

func (c *Client) identity(id RequestID) (*pb.RequestIdentity, error) {
	if id.ClientID != c.id || id.Sequence == 0 { return nil, errors.New("client: invalid request identity") }
	for {
		old := c.sequence.Load()
		if old >= id.Sequence || c.sequence.CompareAndSwap(old, id.Sequence) { break }
	}
	return &pb.RequestIdentity{ClientId: id.ClientID, Sequence: id.Sequence}, nil
}

func (c *Client) retry(ctx context.Context, fn func(context.Context, pb.KVServiceClient) (*pb.KVResponse, error)) (Result, error) {
	index := 0
	if leader := c.leader.Load(); leader > 0 && leader <= uint64(len(c.clients)) { index = int(leader-1) }
	backoff := 20 * time.Millisecond
	for {
		if c.closed.Load() { return Result{}, ErrClosed }
		if err := ctx.Err(); err != nil { return Result{}, err }
		attempt, cancel := context.WithTimeout(ctx, c.timeout)
		response, err := fn(attempt, c.clients[index])
		cancel()
		if err == nil {
			c.leader.Store(uint64(index+1))
			return Result{Value: response.Value, Version: response.Version, Found: response.Found, Swapped: response.Swapped}, nil
		}
		if err := ctx.Err(); err != nil { return Result{}, err }
		if c.closed.Load() { return Result{}, ErrClosed }
		next := (index + 1) % len(c.clients)
		switch status.Code(err) {
		case codes.FailedPrecondition:
			st, _ := status.FromError(err)
			found := false
			for _, detail := range st.Details() {
				if hint, ok := detail.(*pb.NotLeaderDetail); ok {
					found = true
					if target, ok := c.indices[hint.LeaderAddress]; ok { next = target }
				}
			}
			if !found { return Result{}, err }
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		default: return Result{}, err
		}
		index = next
		wait := time.Duration(mathrand.Int64N(int64(backoff)+1))
		timer := time.NewTimer(wait)
		select {
		case <-c.done: timer.Stop(); return Result{}, ErrClosed
		case <-ctx.Done(): timer.Stop(); return Result{}, ctx.Err()
		case <-timer.C:
		}
		if backoff < time.Second { backoff *= 2; if backoff > time.Second { backoff = time.Second } }
	}
}

func (c *Client) Get(ctx context.Context, key string) (Result, error) {
	id, err := c.NewRequest(); if err != nil { return Result{}, err }; return c.GetWithID(ctx, id, key)
}
func (c *Client) GetWithID(ctx context.Context, id RequestID, key string) (Result, error) {
	identity, err := c.identity(id); if err != nil { return Result{}, err }
	req := &pb.GetRequest{Identity: identity, Key: key}
	return c.retry(requestContext(ctx, id), func(ctx context.Context, client pb.KVServiceClient) (*pb.KVResponse, error) { return client.Get(ctx, req) })
}

// ReadLatest uses ReadIndex without a deduplicated request ID. A retry is a
// fresh read and can return a different value from an earlier attempt.
func (c *Client) ReadLatest(ctx context.Context, key string) (Result, error) {
	req := &pb.ReadLatestRequest{Key: key}
	return c.retry(ctx, func(ctx context.Context, client pb.KVServiceClient) (*pb.KVResponse, error) { return client.ReadLatest(ctx, req) })
}
func (c *Client) Put(ctx context.Context, key, value string) (Result, error) {
	id, err := c.NewRequest(); if err != nil { return Result{}, err }; return c.PutWithID(ctx, id, key, value)
}
func (c *Client) PutWithID(ctx context.Context, id RequestID, key, value string) (Result, error) {
	identity, err := c.identity(id); if err != nil { return Result{}, err }
	req := &pb.PutRequest{Identity: identity, Key: key, Value: value}
	return c.retry(requestContext(ctx, id), func(ctx context.Context, client pb.KVServiceClient) (*pb.KVResponse, error) { return client.Put(ctx, req) })
}
func (c *Client) Delete(ctx context.Context, key string) (Result, error) {
	id, err := c.NewRequest(); if err != nil { return Result{}, err }; return c.DeleteWithID(ctx, id, key)
}
func (c *Client) DeleteWithID(ctx context.Context, id RequestID, key string) (Result, error) {
	identity, err := c.identity(id); if err != nil { return Result{}, err }
	req := &pb.DeleteRequest{Identity: identity, Key: key}
	return c.retry(requestContext(ctx, id), func(ctx context.Context, client pb.KVServiceClient) (*pb.KVResponse, error) { return client.Delete(ctx, req) })
}
func (c *Client) CompareAndSwap(ctx context.Context, key string, expected uint64, value string) (Result, error) {
	id, err := c.NewRequest(); if err != nil { return Result{}, err }; return c.CompareAndSwapWithID(ctx, id, key, expected, value)
}
func (c *Client) CompareAndSwapWithID(ctx context.Context, id RequestID, key string, expected uint64, value string) (Result, error) {
	identity, err := c.identity(id); if err != nil { return Result{}, err }
	req := &pb.CompareAndSwapRequest{Identity: identity, Key: key, ExpectedVersion: expected, Value: value}
	return c.retry(requestContext(ctx, id), func(ctx context.Context, client pb.KVServiceClient) (*pb.KVResponse, error) { return client.CompareAndSwap(ctx, req) })
}

// Status queries each configured endpoint; per-node failures are retained in
// the result rather than hiding the status of reachable nodes.
func (c *Client) Status(ctx context.Context) ([]NodeStatus, error) {
	states := make([]NodeStatus, 0, len(c.clients))
	for i, client := range c.clients {
		if c.closed.Load() { return states, ErrClosed }
		if err := ctx.Err(); err != nil { return states, err }
		attempt, cancel := context.WithTimeout(ctx, c.timeout)
		r, err := client.Status(attempt, &pb.StatusRequest{})
		cancel()
		state := NodeStatus{Address: c.addresses[i], Err: err}
		if err == nil {
			state.NodeID, state.Role, state.Term, state.CommitIndex, state.LeaderKnown, state.LeaderID, state.LeaderAddress = r.NodeId, r.Role, r.Term, r.CommitIndex, r.LeaderKnown, r.LeaderId, r.LeaderAddress
			state.VoterIDs = append([]uint64(nil), r.VoterIds...)
		}
		states = append(states, state)
	}
	return states, ctx.Err()
}
