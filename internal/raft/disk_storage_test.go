package raft

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/you/raftkv/internal/transport"
)

func TestDiskStorageRestartAndTornTail(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	hard := HardState{Term: 7, VotedFor: 3, HasVote: true}
	if err := s.Save(hard); err != nil { t.Fatal(err) }
	entries := []transport.LogEntry{{Term: 1, Command: []byte("one")}, {Term: 7, Command: []byte("two")}}
	if err := s.SaveLog(entries); err != nil { t.Fatal(err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
	path := filepath.Join(dir, "log")
	before, err := os.Stat(path)
	if err != nil { t.Fatal(err) }
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil { t.Fatal(err) }
	if _, err = f.Write([]byte("RKL1\x00")); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }
	s, err = OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	defer s.Close()
	got, err := s.Load()
	if err != nil || got != hard { t.Fatalf("state %+v, %v", got, err) }
	log, err := s.LoadLog()
	if err != nil || !reflect.DeepEqual(log, entries) { t.Fatalf("log %+v, %v", log, err) }
	after, err := os.Stat(path)
	if err != nil || after.Size() != before.Size() { t.Fatalf("torn tail not truncated: %v %v", after, err) }
	entries[1] = transport.LogEntry{Term: 8, Command: []byte("replacement")}
	if err := s.SaveLog(entries); err != nil { t.Fatal(err) }
	log, err = s.LoadLog()
	if err != nil || !reflect.DeepEqual(log, entries) { t.Fatalf("replaced log %+v, %v", log, err) }
}

func TestDiskStorageTornPayloadAndCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	if err := s.SaveLog([]transport.LogEntry{{Term: 1, Command: []byte("first")}}); err != nil { t.Fatal(err) }
	path := filepath.Join(dir, "log")
	first, err := os.Stat(path)
	if err != nil { t.Fatal(err) }
	if err := s.SaveLog([]transport.LogEntry{{Term: 1, Command: []byte("first")}, {Term: 2, Command: []byte("second")}}); err != nil { t.Fatal(err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
	second, err := os.Stat(path)
	if err != nil { t.Fatal(err) }
	if err := os.Truncate(path, second.Size()-1); err != nil { t.Fatal(err) }
	s, err = OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	entries, err := s.LoadLog()
	if err != nil || len(entries) != 1 || string(entries[0].Command) != "first" { t.Fatalf("recovered %+v, %v", entries, err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
	data, err := os.ReadFile(path)
	if err != nil || int64(len(data)) != first.Size() { t.Fatalf("tail length %d, %v", len(data), err) }
	data[len(data)-1] ^= 1
	if err := os.WriteFile(path, data, 0600); err != nil { t.Fatal(err) }
	if s, err = OpenDiskStorage(dir); err == nil { _ = s.Close(); t.Fatal("accepted corrupt complete record") }
}

func TestDiskStorageSurvivesProcessExit(t *testing.T) {
	if dir := os.Getenv("RAFTKV_CRASH_CHILD"); dir != "" {
		s, err := OpenDiskStorage(dir)
		if err != nil { os.Exit(10) }
		if s.Save(HardState{Term: 9, VotedFor: 2, HasVote: true}) != nil { os.Exit(11) }
		if s.SaveLog([]transport.LogEntry{{Term: 9, Command: []byte("durable")}}) != nil { os.Exit(12) }
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDiskStorageSurvivesProcessExit$")
	cmd.Env = append(os.Environ(), "RAFTKV_CRASH_CHILD="+dir)
	if out, err := cmd.CombinedOutput(); err != nil { t.Fatalf("child: %v: %s", err, out) }
	s, err := OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	defer s.Close()
	hard, err := s.Load()
	if err != nil || hard.Term != 9 || !hard.HasVote || hard.VotedFor != 2 { t.Fatalf("state %+v, %v", hard, err) }
	entries, err := s.LoadLog()
	if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].Command, []byte("durable")) { t.Fatalf("log %+v, %v", entries, err) }
}

func TestDiskStorageRejectsCorruptState(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStorage(dir)
	if err != nil { t.Fatal(err) }
	if err := s.Save(HardState{Term: 1}); err != nil { t.Fatal(err) }
	if err := s.Close(); err != nil { t.Fatal(err) }
	path := filepath.Join(dir, "state")
	data, err := os.ReadFile(path)
	if err != nil { t.Fatal(err) }
	data[7] ^= 1
	if err := os.WriteFile(path, data, 0600); err != nil { t.Fatal(err) }
	if s, err = OpenDiskStorage(dir); err == nil { _ = s.Close(); t.Fatal("accepted corrupt state") }
}
