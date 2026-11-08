package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/you/raftkv/client"
)

func run(ctx context.Context, args []string, output, errorOutput io.Writer) (runErr error) {
	flags := flag.NewFlagSet("raftctl", flag.ContinueOnError)
	flags.SetOutput(errorOutput)
	endpoints := flags.String("endpoints", "127.0.0.1:21801,127.0.0.1:21803,127.0.0.1:21805", "comma-separated client endpoints")
	timeout := flags.Duration("timeout", 10*time.Second, "overall operation timeout")
	clientID := flags.String("client-id", "", "persistent client ID (requires -sequence for operations)")
	sequence := flags.Uint64("sequence", 0, "request sequence to use with -client-id")
	after := flags.Uint64("after-revision", 0, "watch cursor (exclusive)")
	if err := flags.Parse(args); err != nil { return err }
	if *timeout < 0 || flags.NArg() == 0 { return errors.New("usage: raftctl [flags] get|put|delete|cas|status|member|bench|watch") }
	command, values := flags.Arg(0), flags.Args()[1:]
	if *sequence > 0 && *clientID == "" { return errors.New("-sequence requires -client-id") }
	if *clientID != "" && *sequence == 0 && command != "status" && command != "member" && command != "watch" { return errors.New("-client-id requires a positive -sequence") }
	initial := uint64(0)
	if *sequence > 0 { initial = *sequence - 1 }
	c, err := client.New(client.Config{Endpoints: strings.Split(*endpoints, ","), ClientID: *clientID, InitialSequence: initial})
	if err != nil { return err }
	defer func() { runErr = errors.Join(runErr, c.Close()) }()
	timeoutSet := false
	flags.Visit(func(f *flag.Flag) { if f.Name == "timeout" { timeoutSet = true } })
	if command != "watch" && *timeout == 0 { return errors.New("timeout must be positive") }
	if command == "watch" && !timeoutSet { *timeout = 0 }
	cancel := func() {}
	if *timeout > 0 { ctx, cancel = context.WithTimeout(ctx, *timeout) }
	defer cancel()
	var result client.Result
	switch command {
	case "get":
		if len(values) != 1 { return errors.New("usage: get key") }
		result, err = c.Get(ctx, values[0])
	case "put":
		if len(values) != 2 { return errors.New("usage: put key value") }
		result, err = c.Put(ctx, values[0], values[1])
	case "delete":
		if len(values) != 1 { return errors.New("usage: delete key") }
		result, err = c.Delete(ctx, values[0])
	case "cas":
		if len(values) != 3 { return errors.New("usage: cas key expected-version value") }
		version, parseErr := strconv.ParseUint(values[1], 10, 64)
		if parseErr != nil { return fmt.Errorf("invalid expected version: %w", parseErr) }
		result, err = c.CompareAndSwap(ctx, values[0], version, values[2])
	case "status":
		if len(values) != 0 { return errors.New("usage: status") }
		states, statusErr := c.Status(ctx)
		for _, state := range states {
			entry := struct { Address string; NodeID uint64; Role string; Term, CommitIndex uint64; LeaderKnown bool; LeaderID uint64; LeaderAddress, Error string }{Address: state.Address, NodeID: state.NodeID, Role: state.Role, Term: state.Term, CommitIndex: state.CommitIndex, LeaderKnown: state.LeaderKnown, LeaderID: state.LeaderID, LeaderAddress: state.LeaderAddress}
			if state.Err != nil { entry.Error = state.Err.Error() }
			if err := json.NewEncoder(output).Encode(entry); err != nil { return err }
		}
		return statusErr
	case "member":
		if len(values) != 1 || values[0] != "list" { return errors.New("usage: member list (membership changes are not supported)") }
		states, err := c.Status(ctx)
		if err != nil { return err }
		members, err := configuredMembers(states)
		if err != nil { return err }
		return json.NewEncoder(output).Encode(members)
	case "bench":
		if len(values) != 2 { return errors.New("usage: bench count key") }
		count, parseErr := strconv.Atoi(values[0])
		if parseErr != nil || count < 1 || count > 1000000 { return errors.New("bench count must be between 1 and 1000000") }
		return bench(ctx, c, count, values[1], output)
	case "watch":
		if len(values) != 1 { return errors.New("usage: watch prefix") }
		return c.Watch(ctx, values[0], *after, func(event client.WatchEvent) error { return json.NewEncoder(output).Encode(event) })
	default: return fmt.Errorf("unknown command %q", command)
	}
	if err != nil { return err }
	return json.NewEncoder(output).Encode(result)
}

type memberInfo struct {
	NodeID uint64 `json:"node_id"`
	Address string `json:"address"`
	Role string `json:"role"`
	Reachable bool `json:"reachable"`
}

func configuredMembers(states []client.NodeStatus) ([]memberInfo, error) {
	var leader *client.NodeStatus
	for i := range states {
		state := &states[i]
		if state.Err != nil || state.Role != "leader" { continue }
		if leader == nil || state.Term > leader.Term { leader = state; continue }
		if state.Term == leader.Term && state.NodeID != leader.NodeID { return nil, errors.New("multiple leaders reported in the same term") }
	}
	if leader == nil || len(leader.VoterIDs) == 0 { return nil, errors.New("no leader with a configured voter list is available") }
	members := make([]memberInfo, 0, len(leader.VoterIDs))
	for _, id := range leader.VoterIDs {
		member := memberInfo{NodeID: id}
		for _, state := range states {
			if state.Err == nil && state.NodeID == id { member.Address, member.Role, member.Reachable = state.Address, state.Role, true; break }
		}
		members = append(members, member)
	}
	return members, nil
}

func bench(ctx context.Context, c *client.Client, count int, key string, output io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next, completed atomic.Int64
	var workers sync.WaitGroup
	errs := make(chan error, 2)
	start := time.Now()
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for next.Add(1) <= int64(count) {
				if _, err := c.Get(ctx, key); err != nil { errs <- err; cancel(); return }
				completed.Add(1)
			}
		}()
	}
	workers.Wait()
	close(errs)
	for err := range errs { if err != nil { return fmt.Errorf("bench completed %d/%d: %w", completed.Load(), count, err) } }
	elapsed := time.Since(start)
	return json.NewEncoder(output).Encode(struct { Operations int64; Seconds, OperationsPerSecond float64 }{completed.Load(), elapsed.Seconds(), float64(completed.Load()) / elapsed.Seconds()})
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
