package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/you/raftkv/internal/transport"
)

const maxRecordSize = 64 << 20

type logMutation struct {
	Prefix uint64
	Entries []transport.LogEntry
}

// DiskStorage owns one directory; callers must not open it concurrently from
// another process or instance. The directory must already exist.
type DiskStorage struct {
	dir string
	file *os.File
	state HardState
	log []transport.LogEntry
	err error
	ops chan diskOp
	done chan struct{}
	closed atomic.Bool
}

type diskOp struct { fn func(); stop bool }

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil { return err }
	defer f.Close()
	return f.Sync()
}

func OpenDiskStorage(dir string) (*DiskStorage, error) {
	s := &DiskStorage{dir: dir}
	state, err := os.ReadFile(filepath.Join(dir, "state"))
	if err != nil && !errors.Is(err, os.ErrNotExist) { return nil, err }
	if err == nil {
		if len(state) != 25 || string(state[:4]) != "RKS1" || crc32.ChecksumIEEE(state[:21]) != binary.BigEndian.Uint32(state[21:]) || state[20] > 1 {
			return nil, errors.New("raft: corrupt state file")
		}
		s.state = HardState{Term: binary.BigEndian.Uint64(state[4:12]), VotedFor: transport.NodeID(binary.BigEndian.Uint64(state[12:20])), HasVote: state[20] == 1}
	}
	s.file, err = os.OpenFile(filepath.Join(dir, "log"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil { return nil, err }
	if err = syncDir(dir); err == nil { err = s.recoverLog() }
	if err != nil { _ = s.file.Close(); return nil, err }
	s.ops = make(chan diskOp)
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		for op := range s.ops {
			op.fn()
			if op.stop { return }
		}
	}()
	return s, nil
}

func diskCall[T any](s *DiskStorage, fn func() (T, error)) (T, error) {
	var zero T
	if s.closed.Load() { return zero, ErrStopped }
	reply := make(chan result[T], 1)
	select {
	case s.ops <- diskOp{fn: func() { value, err := fn(); reply <- result[T]{value, err} }}:
	case <-s.done: return zero, ErrStopped
	}
	select {
	case r := <-reply: return r.value, r.err
	case <-s.done:
		select { case r := <-reply: return r.value, r.err; default: return zero, ErrStopped }
	}
}

func (s *DiskStorage) recoverLog() error {
	var offset int64
	for {
		var header [16]byte
		n, err := io.ReadFull(s.file, header[:])
		if err == io.EOF && n == 0 { break }
		if err == io.ErrUnexpectedEOF { return s.trimTail(offset) }
		if err != nil { return err }
		if string(header[:4]) != "RKL1" || crc32.ChecksumIEEE(header[:12]) != binary.BigEndian.Uint32(header[12:]) {
			return fmt.Errorf("raft: corrupt log header at %d", offset)
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if size > maxRecordSize { return fmt.Errorf("raft: log record too large at %d", offset) }
		payload := make([]byte, int(size))
		_, err = io.ReadFull(s.file, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF { return s.trimTail(offset) }
		if err != nil { return err }
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[8:12]) { return fmt.Errorf("raft: corrupt log record at %d", offset) }
		var mutation logMutation
		if err := json.Unmarshal(payload, &mutation); err != nil { return fmt.Errorf("raft: invalid log record at %d: %w", offset, err) }
		if mutation.Prefix > uint64(len(s.log)) { return fmt.Errorf("raft: invalid log prefix at %d", offset) }
		s.log = append(s.log[:mutation.Prefix], mutation.Entries...)
		offset += int64(len(header)) + int64(size)
	}
	_, err := s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *DiskStorage) trimTail(offset int64) error {
	if err := s.file.Truncate(offset); err != nil { return err }
	if err := s.file.Sync(); err != nil { return err }
	_, err := s.file.Seek(offset, io.SeekStart)
	return err
}

func (s *DiskStorage) Load() (HardState, error) {
	return diskCall(s, func() (HardState, error) { return s.state, s.err })
}

func (s *DiskStorage) Save(state HardState) error {
	_, err := diskCall(s, func() (struct{}, error) { return struct{}{}, s.save(state) })
	return err
}

func (s *DiskStorage) save(state HardState) error {
	if s.err != nil { return s.err }
	var data [25]byte
	copy(data[:4], "RKS1")
	binary.BigEndian.PutUint64(data[4:12], state.Term)
	binary.BigEndian.PutUint64(data[12:20], uint64(state.VotedFor))
	if state.HasVote { data[20] = 1 }
	binary.BigEndian.PutUint32(data[21:], crc32.ChecksumIEEE(data[:21]))
	s.err = s.saveState(data[:])
	if s.err == nil { s.state = state }
	return s.err
}

func (s *DiskStorage) saveState(data []byte) error {
	f, err := os.CreateTemp(s.dir, ".state-*")
	if err != nil { return err }
	name := f.Name()
	defer os.Remove(name)
	_, err = io.Copy(f, bytes.NewReader(data))
	if err == nil { err = f.Sync() }
	closeErr := f.Close()
	if err == nil { err = closeErr }
	if err != nil { return err }
	if err = os.Rename(name, filepath.Join(s.dir, "state")); err != nil { return err }
	return syncDir(s.dir)
}

func (s *DiskStorage) LoadLog() ([]transport.LogEntry, error) {
	return diskCall(s, func() ([]transport.LogEntry, error) { return cloneLog(s.log), s.err })
}

func (s *DiskStorage) SaveLog(entries []transport.LogEntry) error {
	entries = cloneLog(entries)
	_, err := diskCall(s, func() (struct{}, error) { return struct{}{}, s.saveLog(entries) })
	return err
}

func (s *DiskStorage) saveLog(entries []transport.LogEntry) error {
	if s.err != nil { return s.err }
	prefix := 0
	for prefix < len(s.log) && prefix < len(entries) && s.log[prefix].Term == entries[prefix].Term && bytes.Equal(s.log[prefix].Command, entries[prefix].Command) { prefix++ }
	if prefix == len(s.log) && prefix == len(entries) { return nil }
	payload, err := json.Marshal(logMutation{Prefix: uint64(prefix), Entries: entries[prefix:]})
	if err != nil { return err }
	if len(payload) > maxRecordSize { return errors.New("raft: log mutation exceeds maximum record size") }
	var header [16]byte
	copy(header[:4], "RKL1")
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[8:12], crc32.ChecksumIEEE(payload))
	binary.BigEndian.PutUint32(header[12:16], crc32.ChecksumIEEE(header[:12]))
	_, err = io.Copy(s.file, bytes.NewReader(header[:]))
	if err == nil { _, err = io.Copy(s.file, bytes.NewReader(payload)) }
	if err == nil { err = s.file.Sync() }
	if err != nil { s.err = err; return err }
	s.log = cloneLog(entries)
	return nil
}

func (s *DiskStorage) Close() error {
	if !s.closed.CompareAndSwap(false, true) { <-s.done; return nil }
	reply := make(chan error, 1)
	s.ops <- diskOp{stop: true, fn: func() { reply <- s.file.Close() }}
	err := <-reply
	<-s.done
	return err
}
