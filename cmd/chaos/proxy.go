package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type connectionPair struct { inbound, outbound net.Conn }

type proxy struct {
	listener net.Listener
	target string
	mu sync.Mutex
	blocked bool
	delay time.Duration
	active map[*connectionPair]struct{}
	done chan struct{}
	acceptDone chan struct{}
	acceptErr error
	workers sync.WaitGroup
}

func newProxy(address, target string) (*proxy, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil { return nil, err }
	p := &proxy{listener: listener, target: target, active: make(map[*connectionPair]struct{}), done: make(chan struct{}), acceptDone: make(chan struct{})}
	go p.accept()
	return p, nil
}

func (p *proxy) accept() {
	defer close(p.acceptDone)
	for {
		conn, err := p.listener.Accept()
		if err != nil { if !errors.Is(err, net.ErrClosed) { p.acceptErr = err }; return }
		p.workers.Add(1)
		go func() { defer p.workers.Done(); p.handle(conn) }()
	}
}

func (p *proxy) handle(in net.Conn) {
	defer in.Close()
	out, err := net.DialTimeout("tcp", p.target, 500*time.Millisecond)
	if err != nil { return }
	defer out.Close()
	pair := &connectionPair{inbound: in, outbound: out}
	p.mu.Lock()
	if p.blocked { p.mu.Unlock(); return }
	p.active[pair] = struct{}{}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.active, pair); p.mu.Unlock() }()
	done := make(chan struct{})
	go func() { p.copy(out, in); _ = out.Close(); _ = in.Close(); close(done) }()
	p.copy(in, out)
	_ = in.Close()
	_ = out.Close()
	<-done
}

func (p *proxy) copy(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock(); delay := p.delay; p.mu.Unlock()
			if delay > 0 {
				timer := time.NewTimer(delay)
				select { case <-timer.C: case <-p.done: timer.Stop(); return }
			}
			if _, writeErr := io.Copy(dst, bytes.NewReader(buf[:n])); writeErr != nil { return }
		}
		if err != nil { return }
	}
}

func (p *proxy) setBlocked(blocked bool) {
	p.mu.Lock()
	p.blocked = blocked
	var pairs []*connectionPair
	if blocked { for pair := range p.active { pairs = append(pairs, pair) } }
	p.mu.Unlock()
	for _, pair := range pairs { _ = pair.inbound.Close(); _ = pair.outbound.Close() }
}

func (p *proxy) setDelay(delay time.Duration) {
	p.mu.Lock()
	p.delay = delay
	p.mu.Unlock()
}

func (p *proxy) close() error {
	err := p.listener.Close()
	<-p.acceptDone
	close(p.done)
	p.setBlocked(true)
	p.workers.Wait()
	return errors.Join(err, p.acceptErr)
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select { case <-ctx.Done(): return false; case <-timer.C: return true }
}
