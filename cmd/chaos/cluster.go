package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const nodeCount = 5

func address(port int) string { return fmt.Sprintf("127.0.0.1:%d", port) }
func raftAddress(node int) string { return address(21800 + node) }
func clientAddress(node int) string { return address(21805 + node) }
func clientProxyAddress(node int) string { return address(21810 + node) }
func linkAddress(from, to int) string { return address(21815 + from*nodeCount + to) }

type nodeProcess struct {
	command *exec.Cmd
	log *os.File
}

type cluster struct {
	root, binary string
	nodes [nodeCount]nodeProcess
	links [nodeCount][nodeCount]*proxy
	clientLinks [nodeCount]*proxy
}

func newCluster(root, binary string) (*cluster, error) {
	c := &cluster{root: root, binary: binary}
	for i := 0; i < nodeCount; i++ {
		p, err := newProxy(clientProxyAddress(i), clientAddress(i))
		if err != nil { return c, err }
		c.clientLinks[i] = p
		for j := 0; j < nodeCount; j++ {
			if i == j { continue }
			p, err := newProxy(linkAddress(i, j), raftAddress(j))
			if err != nil { return c, err }
			c.links[i][j] = p
		}
	}
	for i := 0; i < nodeCount; i++ {
		if err := c.start(i); err != nil { return c, err }
	}
	return c, nil
}

func (c *cluster) start(i int) error {
	if c.nodes[i].command != nil { return fmt.Errorf("node %d already running", i+1) }
	peers := make([]string, 0, nodeCount)
	for j := 0; j < nodeCount; j++ {
		raft := raftAddress(j)
		if i != j { raft = linkAddress(i, j) }
		peers = append(peers, fmt.Sprintf("%d=%s@%s", j+1, raft, clientProxyAddress(j)))
	}
	log, err := os.OpenFile(filepath.Join(c.root, fmt.Sprintf("node-%d.log", i+1)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil { return err }
	cmd := exec.Command(c.binary, "-id", fmt.Sprint(i+1), "-raft-listen", raftAddress(i), "-client-listen", clientAddress(i), "-peers", strings.Join(peers, ","), "-data-dir", filepath.Join(c.root, fmt.Sprintf("node-%d", i+1)))
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil { _ = log.Close(); return err }
	c.nodes[i] = nodeProcess{command: cmd, log: log}
	return nil
}

func (c *cluster) kill(i int) error {
	node := c.nodes[i]
	if node.command == nil { return fmt.Errorf("node %d is not running", i+1) }
	killErr := node.command.Process.Kill()
	if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) { return fmt.Errorf("kill node %d (pid %d): %w", i+1, node.command.Process.Pid, killErr) }
	waitErr := node.command.Wait()
	closeErr := node.log.Close()
	c.nodes[i] = nodeProcess{}
	if killErr != nil { return errors.Join(fmt.Errorf("kill node %d: %w", i+1, killErr), closeErr) }
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) { return errors.Join(fmt.Errorf("node %d did not report killed exit: %v", i+1, waitErr), closeErr) }
	state, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !state.Signaled() || state.Signal() != syscall.SIGKILL { return errors.Join(fmt.Errorf("node %d exited unexpectedly: %w", i+1, waitErr), closeErr) }
	return closeErr
}

func (c *cluster) hasProcesses() bool {
	for _, node := range c.nodes { if node.command != nil { return true } }
	return false
}

func (c *cluster) partition(groups [nodeCount]bool, blocked bool) {
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j && groups[i] != groups[j] { c.links[i][j].setBlocked(blocked) }
		}
	}
}

func (c *cluster) latency(delay time.Duration) {
	for i := 0; i < nodeCount; i++ {
		c.clientLinks[i].setDelay(delay)
		for j := 0; j < nodeCount; j++ { if i != j { c.links[i][j].setDelay(delay) } }
	}
}

func (c *cluster) fault(ctx context.Context, random *rand.Rand, kind int) (string, error) {
	switch kind {
	case 0:
		i := random.Intn(nodeCount)
		if err := c.kill(i); err != nil { return "kill_restart", err }
		wait(ctx, 300*time.Millisecond)
		return "kill_restart", c.start(i)
	case 1:
		order := random.Perm(nodeCount)
		var groups [nodeCount]bool
		groups[order[0]], groups[order[1]] = true, true
		c.partition(groups, true)
		defer c.partition(groups, false)
		wait(ctx, 700*time.Millisecond)
		return "partition", nil
	case 2:
		c.latency(time.Duration(40+random.Intn(41)) * time.Millisecond)
		defer c.latency(0)
		wait(ctx, 700*time.Millisecond)
		return "latency", nil
	case 3:
		i := random.Intn(nodeCount)
		process := c.nodes[i].command.Process
		if err := process.Signal(syscall.SIGSTOP); err != nil { return "freeze", err }
		wait(ctx, 700*time.Millisecond)
		return "freeze", process.Signal(syscall.SIGCONT)
	default: return "invalid", fmt.Errorf("unknown fault kind %d", kind)
	}
}

func (c *cluster) close() error {
	var result error
	for i := 0; i < nodeCount; i++ {
		if c.nodes[i].command != nil { result = errors.Join(result, c.kill(i)) }
	}
	for i := 0; i < nodeCount; i++ {
		if c.clientLinks[i] != nil { result = errors.Join(result, c.clientLinks[i].close()); c.clientLinks[i] = nil }
		for j := 0; j < nodeCount; j++ {
			if c.links[i][j] != nil { result = errors.Join(result, c.links[i][j].close()); c.links[i][j] = nil }
		}
	}
	return result
}
