package client

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type testProcess struct { command *exec.Cmd; stderr bytes.Buffer; waited bool }

func TestClientRecoversAfterLeaderProcessKilled(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "raftkv-server")
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/raftkv-server")
	build.Dir = ".."
	if output, err := build.CombinedOutput(); err != nil { t.Fatalf("build server: %v\n%s", err, output) }
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	processes := make([]*testProcess, 0, 3)
	var workers sync.WaitGroup
	var c *Client
	t.Cleanup(func() {
		cancel()
		workers.Wait()
		if c != nil { if err := c.Close(); err != nil { t.Error(err) } }
		for _, p := range processes {
			if !p.waited {
				_ = p.command.Process.Kill()
				_ = p.command.Wait()
				p.waited = true
			}
			if t.Failed() && p.stderr.Len() > 0 { t.Logf("server stderr: %s", p.stderr.String()) }
		}
	})
	peers := "1=127.0.0.1:21880@127.0.0.1:21881,2=127.0.0.1:21882@127.0.0.1:21883,3=127.0.0.1:21884@127.0.0.1:21885"
	endpoints := []string{"127.0.0.1:21881", "127.0.0.1:21883", "127.0.0.1:21885"}
	for i := 0; i < 3; i++ {
		p := &testProcess{}
		p.command = exec.Command(binary, "-id", fmt.Sprint(i+1), "-raft-listen", fmt.Sprintf("127.0.0.1:%d", 21880+i*2), "-client-listen", endpoints[i], "-peers", peers, "-data-dir", filepath.Join(root, fmt.Sprintf("node-%d", i+1)))
		p.command.Stderr = &p.stderr
		if err := p.command.Start(); err != nil { t.Fatal(err) }
		processes = append(processes, p)
	}
	var err error
	c, err = New(Config{Endpoints: endpoints, ClientID: "process-test", RPCTimeout: 300*time.Millisecond})
	if err != nil { t.Fatal(err) }
	leader := -1
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && leader < 0 {
		states, err := c.Status(ctx)
		if err != nil { t.Fatal(err) }
		for i, state := range states { if state.Err == nil && state.Role == "leader" { leader = i; break } }
		if leader < 0 { time.Sleep(20 * time.Millisecond) }
	}
	if leader < 0 { t.Fatal("no leader elected") }
	ready := make(chan error, 2)
	started := make(chan struct{}, 2)
	results := make(chan error, 2)
	resume := make(chan struct{})
	for i := 0; i < 2; i++ {
		i := i
		workers.Add(1)
		go func() {
			defer workers.Done()
			key := fmt.Sprintf("key-%d", i)
			first, err := c.Put(ctx, key, "value-0")
			if err == nil && first.Version != 1 { err = fmt.Errorf("first version = %d", first.Version) }
			ready <- err
			if err != nil { results <- err; return }
			select { case <-resume: case <-ctx.Done(): results <- ctx.Err(); return }
			for j := 1; j <= 10; j++ {
				if j == 1 { started <- struct{}{} }
				result, err := c.Put(ctx, key, fmt.Sprintf("value-%d", j))
				if err != nil { results <- err; return }
				if result.Version != uint64(j+1) { results <- fmt.Errorf("%s version=%d, want %d", key, result.Version, j+1); return }
			}
			got, err := c.Get(ctx, key)
			if err == nil && (!got.Found || got.Value != "value-10" || got.Version != 11) { err = fmt.Errorf("%s final value %+v", key, got) }
			results <- err
		}()
	}
	for i := 0; i < 2; i++ { if err := <-ready; err != nil { t.Fatal(err) } }
	close(resume)
	for i := 0; i < 2; i++ { <-started }
	if err := processes[leader].command.Process.Kill(); err != nil { t.Fatal(err) }
	_ = processes[leader].command.Wait()
	processes[leader].waited = true
	workers.Wait()
	for i := 0; i < 2; i++ { if err := <-results; err != nil { t.Fatal(err) } }
}
