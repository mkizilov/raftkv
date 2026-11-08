package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

type node struct { command *exec.Cmd; log *os.File }
type cluster struct { nodes []node; endpoints []string }

func startCluster(binary, dir string, count int, noFsync bool) (*cluster, error) {
	c := &cluster{}
	peers := make([]string, 0, count)
	for i := 0; i < count; i++ {
		raft := fmt.Sprintf("127.0.0.1:%d", 21800+i*3)
		api := fmt.Sprintf("127.0.0.1:%d", 21801+i*3)
		peers = append(peers, fmt.Sprintf("%d=%s@%s", i+1, raft, api))
		c.endpoints = append(c.endpoints, api)
	}
	for i := 0; i < count; i++ {
		log, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("node-%d.log", i+1)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil { return c, err }
		args := []string{"-id", fmt.Sprint(i+1), "-raft-listen", fmt.Sprintf("127.0.0.1:%d", 21800+i*3), "-client-listen", c.endpoints[i], "-http-listen", fmt.Sprintf("127.0.0.1:%d", 21802+i*3), "-peers", strings.Join(peers, ","), "-data-dir", filepath.Join(dir, fmt.Sprintf("node-%d", i+1)), "-log-level", "error"}
		if noFsync { args = append(args, "-benchmark-no-fsync") }
		cmd := exec.Command(binary, args...)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil { _ = log.Close(); return c, err }
		c.nodes = append(c.nodes, node{command: cmd, log: log})
	}
	return c, nil
}

func (c *cluster) close() error {
	var result error
	for i := range c.nodes {
		n := &c.nodes[i]
		if n.command == nil { continue }
		killErr := n.command.Process.Kill()
		if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) { result = errors.Join(result, fmt.Errorf("kill node %d pid %d: %w", i+1, n.command.Process.Pid, killErr)); continue }
		waitErr := n.command.Wait()
		if killErr != nil { result = errors.Join(result, fmt.Errorf("node %d exited before cleanup: %w", i+1, killErr)) } else {
			var exit *exec.ExitError
			if !errors.As(waitErr, &exit) { result = errors.Join(result, fmt.Errorf("node %d exited unexpectedly: %v", i+1, waitErr)) } else {
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL { result = errors.Join(result, fmt.Errorf("node %d exited unexpectedly: %w", i+1, waitErr)) }
			}
		}
		result = errors.Join(result, n.log.Close())
		n.command = nil
	}
	return result
}

func (c *cluster) running() bool {
	for _, node := range c.nodes { if node.command != nil { return true } }
	return false
}
