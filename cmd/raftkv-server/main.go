package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/internal/kv"
	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/transport"
	grpctransport "github.com/you/raftkv/internal/transport/grpc"
	"google.golang.org/grpc"
)

type config struct {
	id transport.NodeID
	raftListen, clientListen, dataDir string
	raftPeers, clientPeers map[transport.NodeID]string
	snapshotThreshold uint64
}

func parseConfig(args []string, output io.Writer) (config, error) {
	flags := flag.NewFlagSet("raftkv-server", flag.ContinueOnError)
	flags.SetOutput(output)
	id := flags.Uint64("id", 0, "nonzero node ID")
	raftListen := flags.String("raft-listen", "127.0.0.1:21800", "Raft listen address")
	clientListen := flags.String("client-listen", "127.0.0.1:21801", "client listen address")
	dataDir := flags.String("data-dir", "", "node data directory")
	peers := flags.String("peers", "", "comma-separated id=raft-address@client-address entries including self")
	threshold := flags.Uint64("snapshot-bytes", 1<<20, "logical log bytes before snapshot; zero disables")
	if err := flags.Parse(args); err != nil { return config{}, err }
	if flags.NArg() != 0 || *id == 0 || *dataDir == "" || *peers == "" { return config{}, errors.New("id, data-dir and peers are required") }
	c := config{id: transport.NodeID(*id), raftListen: *raftListen, clientListen: *clientListen, dataDir: *dataDir, raftPeers: make(map[transport.NodeID]string), clientPeers: make(map[transport.NodeID]string), snapshotThreshold: *threshold}
	for _, pair := range strings.Split(*peers, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 { return config{}, fmt.Errorf("invalid peer %q", pair) }
		number, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || number == 0 { return config{}, fmt.Errorf("invalid peer ID %q", parts[0]) }
		addresses := strings.Split(parts[1], "@")
		if len(addresses) != 2 { return config{}, fmt.Errorf("invalid peer addresses %q", parts[1]) }
		for _, address := range addresses {
			if host, port, err := net.SplitHostPort(address); err != nil || host == "" || port == "" { return config{}, fmt.Errorf("invalid peer address %q", address) }
		}
		peer := transport.NodeID(number)
		if _, exists := c.raftPeers[peer]; exists { return config{}, fmt.Errorf("duplicate peer %d", peer) }
		c.raftPeers[peer], c.clientPeers[peer] = addresses[0], addresses[1]
	}
	if _, ok := c.raftPeers[c.id]; !ok { return config{}, errors.New("peers must include this node") }
	return c, nil
}

func serve(ctx context.Context, c config) (result error) {
	if err := os.MkdirAll(c.dataDir, 0700); err != nil { return err }
	store, err := raft.OpenDiskStorage(c.dataDir)
	if err != nil { return err }
	defer func() { result = errors.Join(result, store.Close()) }()
	peers := make(map[transport.NodeID]string)
	ids := make([]transport.NodeID, 0, len(c.raftPeers)-1)
	for id, address := range c.raftPeers { if id != c.id { peers[id] = address; ids = append(ids, id) } }
	transportClient, err := grpctransport.New(peers, time.Second)
	if err != nil { return err }
	defer func() { result = errors.Join(result, transportClient.Close()) }()
	service, err := kv.New(raft.Config{ID: c.id, Peers: ids, Transport: transportClient, Storage: store, Seed: int64(c.id)}, c.snapshotThreshold)
	if err != nil { return err }
	defer service.Close()
	raftListener, err := net.Listen("tcp", c.raftListen)
	if err != nil { return err }
	defer raftListener.Close()
	clientListener, err := net.Listen("tcp", c.clientListen)
	if err != nil { return err }
	defer clientListener.Close()
	const maxMessage = 64 << 20
	raftServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxMessage))
	clientServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxMessage))
	pb.RegisterRaftServiceServer(raftServer, &grpctransport.Handler{Node: service.Node()})
	pb.RegisterKVServiceServer(clientServer, &kv.GRPC{Server: service, LeaderAddresses: c.clientPeers})
	service.Start()
	errs := make(chan error, 2)
	go func() { errs <- raftServer.Serve(raftListener) }()
	go func() { errs <- clientServer.Serve(clientListener) }()
	finished := 0
	select {
	case <-ctx.Done():
	case err = <-errs:
		finished++
		if err != nil { result = err } else { result = errors.New("gRPC server stopped unexpectedly") }
	}
	raftServer.Stop()
	clientServer.Stop()
	for finished < 2 {
		err = <-errs
		finished++
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) { result = errors.Join(result, err) }
	}
	return result
}

func main() {
	c, err := parseConfig(os.Args[1:], os.Stderr)
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = serve(ctx, c)
	}
	if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
