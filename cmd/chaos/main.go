package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/you/raftkv/client"
)

type options struct { duration time.Duration; seed int64 }

func runChaos(ctx context.Context, opts options, output io.Writer) (result error) {
	if opts.duration <= 0 { return errors.New("chaos: duration must be positive") }
	h := newHistory()
	faults := map[string]int{"kill_restart": 0, "partition": 0, "latency": 0, "freeze": 0}
	started := time.Now()
	check := "not-run"
	defer func() {
		faultTotal := 0
		for _, count := range faults { faultTotal += count }
		_, err := fmt.Fprintf(output, "seed=%d elapsed=%s operations=%d puts=%d gets=%d cas=%d faults_injected=%d faults=%v linearizability=%s\n", opts.seed, time.Since(started).Round(time.Millisecond), len(h.operations), h.counts["put"], h.counts["get"], h.counts["cas"], faultTotal, faults, check)
		result = errors.Join(result, err)
	}()
	root, err := filepath.Abs(filepath.Join(".agent-local", "tmp"))
	if err != nil { return err }
	if err := os.MkdirAll(root, 0700); err != nil { return err }
	work, err := os.MkdirTemp(root, "chaos-*")
	if err != nil { return err }
	cleanupOK := true
	defer func() {
		if cleanupOK { result = errors.Join(result, os.RemoveAll(work)) } else { result = errors.Join(result, fmt.Errorf("chaos: temporary data retained at %s because shutdown failed", work)) }
	}()
	binary := filepath.Join(work, "raftkv-server")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/raftkv-server")
	build.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	build.Cancel = func() error {
		err := syscall.Kill(-build.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) { return os.ErrProcessDone }
		return err
	}
	build.WaitDelay = 5 * time.Second
	if out, err := build.CombinedOutput(); err != nil { return fmt.Errorf("chaos: build server: %w: %s", err, out) }
	cluster, err := newCluster(work, binary)
	defer func() {
		closeErr := cluster.close()
		if closeErr != nil { result = errors.Join(result, closeErr) }
		if cluster.hasProcesses() { cleanupOK = false }
		if result != nil {
			for i := 0; i < nodeCount; i++ {
				file, err := os.Open(filepath.Join(work, fmt.Sprintf("node-%d.log", i+1)))
				if errors.Is(err, os.ErrNotExist) { continue }
				if err != nil { result = errors.Join(result, err); continue }
				if _, err = file.Seek(-8192, io.SeekEnd); err != nil {
					if _, err = file.Seek(0, io.SeekStart); err != nil { result = errors.Join(result, err, file.Close()); continue }
				}
				data, readErr := io.ReadAll(io.LimitReader(file, 8192))
				closeErr := file.Close()
				if len(data) > 0 { _, err = fmt.Fprintf(output, "node-%d log: %s\n", i+1, data); result = errors.Join(result, err) }
				result = errors.Join(result, readErr, closeErr)
			}
		}
	}()
	if err != nil { return fmt.Errorf("chaos: start cluster: %w", err) }
	endpoints := make([]string, nodeCount)
	for i := range endpoints { endpoints[i] = clientProxyAddress(i) }
	probe, err := client.New(client.Config{Endpoints: endpoints})
	if err != nil { return err }
	defer func() { result = errors.Join(result, probe.Close()) }()
	startup, stopStartup := context.WithTimeout(ctx, 15*time.Second)
	defer stopStartup()
	for {
		states, err := probe.Status(startup)
		if err != nil { return fmt.Errorf("chaos: startup: %w", err) }
		ready := false
		for _, state := range states { if state.Err == nil && state.Role == "leader" { ready = true; break } }
		if ready { break }
		if !wait(startup, 50*time.Millisecond) { return fmt.Errorf("chaos: no leader: %w", startup.Err()) }
	}
	launchCtx, stopLaunching := context.WithTimeout(ctx, opts.duration)
	defer stopLaunching()
	operationCtx, stopOperations := context.WithCancel(ctx)
	defer stopOperations()
	workerErrors := make(chan error, 2)
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		i := i
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := workload(launchCtx, operationCtx, endpoints, opts.seed+int64(i+1), i, h)
			if err != nil { workerErrors <- err; stopLaunching() }
		}()
	}
	random := rand.New(rand.NewSource(opts.seed))
	initialFaults := random.Perm(4)
	faultIndex := 0
	var faultErr error
	for wait(launchCtx, time.Duration(500+random.Intn(1000))*time.Millisecond) {
		kindIndex := random.Intn(4)
		if faultIndex < len(initialFaults) { kindIndex = initialFaults[faultIndex] }
		faultIndex++
		kind, err := cluster.fault(launchCtx, random, kindIndex)
		faults[kind]++
		if err != nil { faultErr = fmt.Errorf("chaos: %s: %w", kind, err); stopLaunching(); break }
	}
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	timer := time.NewTimer(30*time.Second)
	select {
	case <-finished:
	case <-timer.C:
		stopOperations()
		<-finished
		faultErr = errors.Join(faultErr, errors.New("chaos: clients failed to drain within 30 seconds"))
	}
	timer.Stop()
	close(workerErrors)
	for err := range workerErrors { faultErr = errors.Join(faultErr, err) }
	if ctx.Err() != nil { faultErr = errors.Join(faultErr, ctx.Err()) }
	if len(h.operations) == 0 { return errors.Join(faultErr, errors.New("chaos: no completed operations")) }
	verdict, info := porcupine.CheckOperationsVerbose(model(), h.operations, 60*time.Second)
	check = string(verdict)
	if verdict != porcupine.Ok {
		dir := filepath.Join(".agent-local", "chaos-failures")
		if err := os.MkdirAll(dir, 0700); err != nil { return errors.Join(faultErr, fmt.Errorf("chaos: %s; create visualization directory: %w", verdict, err)) }
		file, err := os.CreateTemp(dir, "history-*.html")
		if err != nil { return errors.Join(faultErr, fmt.Errorf("chaos: %s; create visualization: %w", verdict, err)) }
		name := file.Name()
		visualErr := porcupine.Visualize(model(), info, file)
		closeErr := file.Close()
		return errors.Join(faultErr, fmt.Errorf("chaos: linearizability %s; visualization: %s", verdict, name), visualErr, closeErr)
	}
	if faultErr != nil { check = "Ok-incomplete" }
	return faultErr
}

func workload(launch, ctx context.Context, endpoints []string, seed int64, id int, h *history) (resultErr error) {
	c, err := client.New(client.Config{Endpoints: endpoints})
	if err != nil { return err }
	defer func() { resultErr = errors.Join(resultErr, c.Close()) }()
	random := rand.New(rand.NewSource(seed))
	versions := make(map[string]uint64)
	for sequence := 0; launch.Err() == nil; sequence++ {
		key := fmt.Sprintf("key-%d", random.Intn(2))
		op := operation{Key: key}
		kind := random.Intn(3)
		if sequence < 3 { kind = sequence }
		switch kind {
		case 0: op.Kind, op.Value = "put", fmt.Sprintf("client-%d-%d", id, sequence)
		case 1: op.Kind = "get"
		case 2: op.Kind, op.Value, op.Expected = "cas", fmt.Sprintf("client-%d-%d", id, sequence), versions[key]
		}
		start := h.stamp()
		var result client.Result
		switch op.Kind {
		case "put": result, err = c.Put(ctx, key, op.Value)
		case "get": result, err = c.Get(ctx, key)
		case "cas": result, err = c.CompareAndSwap(ctx, key, op.Expected, op.Value)
		}
		end := h.stamp()
		if err != nil { return fmt.Errorf("chaos: client %d %s: %w (outcome may be unknown)", id, op.Kind, err) }
		if err := h.record(id, op, start, end, result); err != nil { return err }
		versions[key] = result.Version
		if !wait(launch, 300*time.Millisecond) { break }
	}
	return nil
}

func main() {
	duration := flag.Duration("duration", 10*time.Minute, "fault-injection duration")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	flag.Parse()
	if flag.NArg() != 0 { fmt.Fprintln(os.Stderr, "chaos: unexpected arguments"); os.Exit(2) }
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runChaos(ctx, options{duration: *duration, seed: *seed}, os.Stdout); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
