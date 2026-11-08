package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/you/raftkv/client"
)

func TestProxyPartitionsExistingConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:21895")
	if err != nil { t.Fatal(err) }
	accepted := make(chan struct{})
	var workers sync.WaitGroup
	go func() {
		defer close(accepted)
		for {
			conn, err := listener.Accept()
			if err != nil { return }
			workers.Add(1)
			go func() { defer workers.Done(); defer conn.Close(); _, _ = io.Copy(conn, conn) }()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-accepted; workers.Wait() })
	p, err := newProxy("127.0.0.1:21896", "127.0.0.1:21895")
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := p.close(); err != nil { t.Error(err) } })
	conn, err := net.DialTimeout("tcp", "127.0.0.1:21896", time.Second)
	if err != nil { t.Fatal(err) }
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2*time.Second))
	p.setDelay(25*time.Millisecond)
	start := time.Now()
	if _, err := conn.Write([]byte("a")); err != nil { t.Fatal(err) }
	var data [1]byte
	if _, err := io.ReadFull(conn, data[:]); err != nil || string(data[:]) != "a" { t.Fatalf("echo %q: %v", data, err) }
	if time.Since(start) < 20*time.Millisecond { t.Fatal("proxy did not delay traffic") }
	p.setBlocked(true)
	if _, err := conn.Read(data[:]); err == nil { t.Fatal("partition left existing connection open") }
	p.setBlocked(false)
	second, err := net.DialTimeout("tcp", "127.0.0.1:21896", time.Second)
	if err != nil { t.Fatal(err) }
	defer second.Close()
	_ = second.SetDeadline(time.Now().Add(2*time.Second))
	if _, err := second.Write([]byte("b")); err != nil { t.Fatal(err) }
	if _, err := io.ReadFull(second, data[:]); err != nil || string(data[:]) != "b" { t.Fatalf("healed echo %q: %v", data, err) }
}

func TestHistoryModelRejectsStaleRead(t *testing.T) {
	history := []porcupine.Operation{
		{Input: operation{Kind: "put", Key: "k", Value: "v"}, Call: 1, Return: 2, Output: client.Result{Found: true, Value: "v", Version: 1}},
		{Input: operation{Kind: "get", Key: "k"}, Call: 3, Return: 4, Output: client.Result{}},
	}
	if porcupine.CheckOperations(model(), history) { t.Fatal("accepted stale read") }
}

func TestChaosSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := runChaos(ctx, options{duration: 3*time.Second, seed: 12345}, &output); err != nil { t.Fatalf("chaos: %v\n%s", err, output.String()) }
	if !strings.Contains(output.String(), "linearizability=Ok") { t.Fatalf("missing verdict: %s", output.String()) }
	if strings.Contains(output.String(), "faults_injected=0") { t.Fatalf("no fault injected: %s", output.String()) }
}
