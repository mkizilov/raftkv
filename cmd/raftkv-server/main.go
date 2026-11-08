package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "github.com/you/raftkv/gen/raftkv/v1"
	"github.com/you/raftkv/internal/kv"
	"github.com/you/raftkv/internal/observability"
	"github.com/you/raftkv/internal/raft"
	"github.com/you/raftkv/internal/transport"
	grpctransport "github.com/you/raftkv/internal/transport/grpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

type config struct {
	id transport.NodeID
	raftListen, clientListen, dataDir string
	httpListen, otlpEndpoint string
	logLevel slog.Level
	installErrorHandler bool
	raftPeers, clientPeers map[transport.NodeID]string
	snapshotThreshold uint64
	benchmarkNoFsync bool
}

func parseConfig(args []string, output io.Writer) (config, error) {
	flags := flag.NewFlagSet("raftkv-server", flag.ContinueOnError)
	flags.SetOutput(output)
	id := flags.Uint64("id", 0, "nonzero node ID")
	raftListen := flags.String("raft-listen", "127.0.0.1:21800", "Raft listen address")
	clientListen := flags.String("client-listen", "127.0.0.1:21801", "client listen address")
	httpListen := flags.String("http-listen", "127.0.0.1:21890", "metrics and health listen address")
	level := flags.String("log-level", "info", "debug, info, warn or error")
	otlp := flags.String("otlp-endpoint", "", "optional OTLP/HTTP traces URL")
	dataDir := flags.String("data-dir", "", "node data directory")
	peers := flags.String("peers", "", "comma-separated id=raft-address@client-address entries including self")
	threshold := flags.Uint64("snapshot-bytes", 1<<20, "logical log bytes before snapshot; zero disables")
	noFsync := flags.Bool("benchmark-no-fsync", false, "UNSAFE benchmark only: disable durable fsync")
	if err := flags.Parse(args); err != nil { return config{}, err }
	if flags.NArg() != 0 || *id == 0 || *dataDir == "" || *peers == "" { return config{}, errors.New("id, data-dir and peers are required") }
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(strings.ToUpper(*level))); err != nil { return config{}, fmt.Errorf("invalid log level %q", *level) }
	c := config{id: transport.NodeID(*id), raftListen: *raftListen, clientListen: *clientListen, httpListen: *httpListen, otlpEndpoint: *otlp, logLevel: logLevel, installErrorHandler: true, dataDir: *dataDir, raftPeers: make(map[transport.NodeID]string), clientPeers: make(map[transport.NodeID]string), snapshotThreshold: *threshold, benchmarkNoFsync: *noFsync}
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
	if c.httpListen == "" { c.httpListen = "127.0.0.1:21890" }
	provider, err := observability.NewTracerProvider(ctx, c.otlpEndpoint)
	if err != nil { observability.New(uint64(c.id), c.logLevel, os.Stderr, nil).Logger.Error("tracing setup failed", "error", err.Error()); return err }
	telemetry := observability.New(uint64(c.id), c.logLevel, os.Stderr, provider)
	if c.installErrorHandler { otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { telemetry.Logger.Error("telemetry error", "error", err.Error()) })) }
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, provider.Shutdown(shutdown))
		if result != nil { telemetry.Logger.Error("server stopped", "error", result.Error()) } else { telemetry.Logger.Info("server stopped") }
	}()
	telemetry.Logger.Info("server starting", "raft_listen", c.raftListen, "client_listen", c.clientListen, "http_listen", c.httpListen)
	if c.benchmarkNoFsync { telemetry.Logger.Error("UNSAFE benchmark mode: fsync disabled") }
	if err := os.MkdirAll(c.dataDir, 0700); err != nil { return err }
	var store *raft.DiskStorage
	if c.benchmarkNoFsync { store, err = raft.OpenDiskStorageBenchmarkNoFsync(c.dataDir) } else { store, err = raft.OpenDiskStorage(c.dataDir) }
	if err != nil { return err }
	defer func() { result = errors.Join(result, store.Close()) }()
	peers := make(map[transport.NodeID]string)
	ids := make([]transport.NodeID, 0, len(c.raftPeers)-1)
	for id, address := range c.raftPeers { if id != c.id { peers[id] = address; ids = append(ids, id) } }
	transportClient, err := grpctransport.New(peers, time.Second)
	if err != nil { return err }
	defer func() { result = errors.Join(result, transportClient.Close()) }()
	service, err := kv.New(raft.Config{ID: c.id, Peers: ids, Transport: transportClient, Storage: store, Seed: int64(c.id), Observer: telemetry}, c.snapshotThreshold)
	if err != nil { return err }
	defer service.Close()
	raftListener, err := net.Listen("tcp", c.raftListen)
	if err != nil { return err }
	defer raftListener.Close()
	clientListener, err := net.Listen("tcp", c.clientListen)
	if err != nil { return err }
	defer clientListener.Close()
	httpListener, err := net.Listen("tcp", c.httpListen)
	if err != nil { return err }
	defer httpListener.Close()
	const maxMessage = 64 << 20
	raftServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxMessage), grpc.ChainUnaryInterceptor(telemetry.Unary), grpc.ChainStreamInterceptor(telemetry.Stream))
	clientServer := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxMessage), grpc.ChainUnaryInterceptor(telemetry.Unary), grpc.ChainStreamInterceptor(telemetry.Stream))
	httpServer := &http.Server{Handler: telemetry.Handler(service.Healthy, func(ctx context.Context) error { _, err := service.ReadLatest(ctx, "__readiness__"); return err }), ReadHeaderTimeout: 5*time.Second, ErrorLog: slog.NewLogLogger(telemetry.Logger.Handler(), slog.LevelError)}
	pb.RegisterRaftServiceServer(raftServer, &grpctransport.Handler{Node: service.Node()})
	pb.RegisterKVServiceServer(clientServer, &kv.GRPC{Server: service, NodeID: c.id, LeaderAddresses: c.clientPeers})
	service.Start()
	errs := make(chan error, 3)
	go func() { errs <- raftServer.Serve(raftListener) }()
	go func() { errs <- clientServer.Serve(clientListener) }()
	go func() { errs <- httpServer.Serve(httpListener) }()
	finished := 0
	select {
	case <-ctx.Done():
	case err = <-errs:
		finished++
		if err != nil { result = err } else { result = errors.New("gRPC server stopped unexpectedly") }
	}
	raftServer.Stop()
	clientServer.Stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := httpServer.Shutdown(shutdown); err != nil { result = errors.Join(result, err, httpServer.Close()) }
	cancel()
	for finished < 3 {
		err = <-errs
		finished++
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, http.ErrServerClosed) { result = errors.Join(result, err) }
	}
	return result
}

func main() {
	c, err := parseConfig(os.Args[1:], io.Discard)
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = serve(ctx, c)
	}
	if err != nil {
		if c.id == 0 { observability.New(0, slog.LevelInfo, os.Stderr, nil).Logger.Error("invalid configuration", "error", err.Error()) }
		os.Exit(1)
	}
}
