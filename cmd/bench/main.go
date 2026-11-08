package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/you/raftkv/client"
)

const bucketWidth = 100 * time.Microsecond
const maxLatency = 5 * time.Second
const bucketCount = int(maxLatency / bucketWidth) + 1
const hardMaxOps = 20000

type histogram struct { buckets [bucketCount]atomic.Uint64; count atomic.Uint64; maximum atomic.Int64 }

func (h *histogram) observe(duration time.Duration) {
	index := int(duration / bucketWidth)
	if index >= len(h.buckets) { index = len(h.buckets)-1 }
	h.buckets[index].Add(1)
	h.count.Add(1)
	for { old := h.maximum.Load(); if int64(duration) <= old || h.maximum.CompareAndSwap(old, int64(duration)) { break } }
}

func (h *histogram) percentile(percent uint64) time.Duration {
	count := h.count.Load()
	if count == 0 { return 0 }
	rank := (count*percent + 99) / 100
	var sum uint64
	for i := range h.buckets {
		sum += h.buckets[i].Load()
		if sum >= rank {
			if i == len(h.buckets)-1 { return time.Duration(h.maximum.Load()) }
			return time.Duration(i+1) * bucketWidth
		}
	}
	return maxLatency
}

type measurement struct {
	Nodes, Clients int
	Fsync bool
	Operation string
	Operations uint64
	Seconds, OperationsPerSecond float64
	P50UpperMicroseconds, P99UpperMicroseconds int64
	StoppedAtLimit bool
}

func measure(ctx context.Context, endpoints []string, nodes, clients int, fsync bool, kind string, duration time.Duration, maxOps uint64) (result measurement, retErr error) {
	result = measurement{Nodes: nodes, Clients: clients, Fsync: fsync, Operation: kind}
	connections := make([]*client.Client, 0, clients)
	for i := 0; i < clients; i++ {
		c, err := client.New(client.Config{Endpoints: endpoints})
		if err != nil { for _, opened := range connections { err = errors.Join(err, opened.Close()) }; return result, err }
		connections = append(connections, c)
	}
	defer func() { for _, c := range connections { retErr = errors.Join(retErr, c.Close()) } }()
	warmup, cancel := context.WithTimeout(ctx, 15*time.Second)
	_, err := connections[0].Put(warmup, "benchmark/key", "initial")
	cancel()
	if err != nil { return result, fmt.Errorf("warmup: %w", err) }
	var samples histogram
	var issued atomic.Uint64
	var workers sync.WaitGroup
	errs := make(chan error, clients)
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	start := time.Now()
	end := start.Add(duration)
	for i, c := range connections {
		i, c := i, c
		workers.Add(1)
		go func() {
			defer workers.Done()
			for time.Now().Before(end) && workCtx.Err() == nil {
				if issued.Add(1) > maxOps { return }
				call, cancel := context.WithTimeout(workCtx, maxLatency)
				begin := time.Now()
				var err error
				switch kind {
				case "put": _, err = c.Put(call, "benchmark/key", fmt.Sprintf("client-%d", i))
				case "get": _, err = c.Get(call, "benchmark/key")
				case "read-index": _, err = c.ReadLatest(call, "benchmark/key")
				default: err = fmt.Errorf("unknown operation %q", kind)
				}
				elapsed := time.Since(begin)
				cancel()
				if err != nil { errs <- err; stop(); return }
				samples.observe(elapsed)
			}
		}()
	}
	workers.Wait()
	elapsed := time.Since(start)
	close(errs)
	for err := range errs { if err != nil { return result, err } }
	if ctx.Err() != nil { return result, ctx.Err() }
	result.Operations = samples.count.Load()
	if result.Operations == 0 { return result, errors.New("benchmark: no completed operations") }
	result.Seconds = elapsed.Seconds()
	result.OperationsPerSecond = float64(result.Operations) / result.Seconds
	result.P50UpperMicroseconds = samples.percentile(50).Microseconds()
	result.P99UpperMicroseconds = samples.percentile(99).Microseconds()
	result.StoppedAtLimit = result.Operations >= maxOps
	return result, nil
}

func run(ctx context.Context, duration time.Duration, maxOps uint64, output io.Writer) (result error) {
	if duration <= 0 || duration > 30*time.Second || maxOps == 0 || maxOps > hardMaxOps { return errors.New("benchmark: duration must be in (0, 30s] and max-ops in [1, 20000]") }
	root, err := filepath.Abs(filepath.Join(".agent-local", "tmp"))
	if err != nil { return err }
	if err := os.MkdirAll(root, 0700); err != nil { return err }
	work, err := os.MkdirTemp(root, "benchmark-*")
	if err != nil { return err }
	cleanupOK := true
	defer func() { if cleanupOK { result = errors.Join(result, os.RemoveAll(work)) } else { result = errors.Join(result, fmt.Errorf("benchmark: data retained at %s because a server could not be stopped", work)) } }()
	binary := filepath.Join(work, "raftkv-server")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/raftkv-server")
	build.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	build.Cancel = func() error { err := syscall.Kill(-build.Process.Pid, syscall.SIGKILL); if errors.Is(err, syscall.ESRCH) { return os.ErrProcessDone }; return err }
	build.WaitDelay = 5*time.Second
	if out, err := build.CombinedOutput(); err != nil { return fmt.Errorf("benchmark: build: %w: %s", err, out) }
	encoder := json.NewEncoder(output)
	for _, nodes := range []int{3, 5} {
		for _, fsync := range []bool{true, false} {
			for _, clients := range []int{1, 16, 128} {
				for _, kind := range []string{"put", "get", "read-index"} {
					dir, err := os.MkdirTemp(work, "case-*")
					if err != nil { return err }
					cluster, err := startCluster(binary, dir, nodes, !fsync)
					if err != nil {
						closeErr := cluster.close()
						if cluster.running() { cleanupOK = false }
						return errors.Join(err, closeErr)
					}
					measurement, runErr := measure(ctx, cluster.endpoints, nodes, clients, fsync, kind, duration, maxOps)
					closeErr := cluster.close()
					if cluster.running() { cleanupOK = false }
					if runErr != nil { return errors.Join(fmt.Errorf("benchmark %d nodes %d clients fsync=%v %s: %w", nodes, clients, fsync, kind, runErr), closeErr) }
					if closeErr != nil { return fmt.Errorf("benchmark cleanup: %w", closeErr) }
					if err := encoder.Encode(measurement); err != nil { return err }
					if err := os.RemoveAll(dir); err != nil { return err }
				}
			}
		}
	}
	return nil
}

func main() {
	duration := flag.Duration("duration", 5*time.Second, "duration per case")
	maxOps := flag.Uint64("max-ops", 20000, "maximum operations per case")
	flag.Parse()
	if flag.NArg() != 0 { fmt.Fprintln(os.Stderr, "benchmark: unexpected arguments"); os.Exit(2) }
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *duration, *maxOps, os.Stdout); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
