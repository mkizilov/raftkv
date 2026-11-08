package kv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/you/raftkv/internal/raft"
)

type historyOutput struct { Result Result; Error string }

type historyRecorder struct {
	mu sync.Mutex
	start time.Time
	clients map[string]int
	ops []porcupine.Operation
}

func newHistory() *historyRecorder { return &historyRecorder{start: time.Now(), clients: make(map[string]int)} }

func (h *historyRecorder) stamp() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Since(h.start).Nanoseconds()
}

func historyError(err error) string {
	switch {
	case err == nil: return ""
	case errors.Is(err, ErrNotLeader): return "not-leader"
	case errors.Is(err, ErrRequestConflict): return "conflict"
	case errors.Is(err, ErrVersionExhausted): return "overflow"
	case errors.Is(err, raft.ErrReadNotReady): return "not-leader"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled): return "unknown"
	default: return err.Error()
	}
}

func (h *historyRecorder) invoke(cmd command, fn func() (Result, error)) (Result, error) {
	start := h.stamp()
	result, err := fn()
	end := h.stamp()
	h.mu.Lock()
	id, ok := h.clients[cmd.ClientID]
	if !ok { id = len(h.clients); h.clients[cmd.ClientID] = id }
	h.ops = append(h.ops, porcupine.Operation{ClientId: id, Input: cmd, Call: start, Return: end, Output: historyOutput{Result: result, Error: historyError(err)}})
	h.mu.Unlock()
	return result, err
}

type recordedServer struct { *Server; history *historyRecorder }

func (s *recordedServer) Get(ctx context.Context, client string, seq uint64, key string) (Result, error) {
	cmd := command{ClientID: client, Sequence: seq, Operation: "get", Key: key}
	return s.history.invoke(cmd, func() (Result, error) { return s.Server.Get(ctx, client, seq, key) })
}
func (s *recordedServer) Put(ctx context.Context, client string, seq uint64, key, value string) (Result, error) {
	cmd := command{ClientID: client, Sequence: seq, Operation: "put", Key: key, Value: value}
	return s.history.invoke(cmd, func() (Result, error) { return s.Server.Put(ctx, client, seq, key, value) })
}
func (s *recordedServer) Delete(ctx context.Context, client string, seq uint64, key string) (Result, error) {
	cmd := command{ClientID: client, Sequence: seq, Operation: "delete", Key: key}
	return s.history.invoke(cmd, func() (Result, error) { return s.Server.Delete(ctx, client, seq, key) })
}
func (s *recordedServer) CompareAndSwap(ctx context.Context, client string, seq uint64, key string, expected uint64, value string) (Result, error) {
	cmd := command{ClientID: client, Sequence: seq, Operation: "cas", Key: key, Expected: expected, Value: value}
	return s.history.invoke(cmd, func() (Result, error) { return s.Server.CompareAndSwap(ctx, client, seq, key, expected, value) })
}
func (s *recordedServer) ReadLatest(ctx context.Context, key string) (Result, error) {
	cmd := command{ClientID: "read-latest", Operation: "latest", Key: key}
	return s.history.invoke(cmd, func() (Result, error) { return s.Server.ReadLatest(ctx, key) })
}

type modelState struct { values map[string]Result; requests map[requestID]record }

func cloneModel(state modelState) modelState {
	copy := modelState{values: make(map[string]Result), requests: make(map[requestID]record)}
	for key, value := range state.values { copy.values[key] = value }
	for key, value := range state.requests { copy.requests[key] = value }
	return copy
}

func modelApply(state modelState, cmd command) (modelState, historyOutput) {
	next := cloneModel(state)
	id := requestID{cmd.ClientID, cmd.Sequence}
	if cmd.Operation != "latest" {
		if saved, ok := next.requests[id]; ok {
			if saved.Command != cmd { return next, historyOutput{Error: "conflict"} }
			return next, historyOutput{Result: saved.Result, Error: saved.Error}
		}
	}
	value := next.values[cmd.Key]
	write := cmd.Operation == "put" || cmd.Operation == "delete" || (cmd.Operation == "cas" && value.Version == cmd.Expected)
	out := historyOutput{}
	if write && value.Version == ^uint64(0) { out.Error = "overflow" } else {
		if write {
			value.Version++
			value.Found = cmd.Operation != "delete"
			value.Value = cmd.Value
			if !value.Found { value.Value = "" }
			next.values[cmd.Key] = value
		}
		out.Result = value
		out.Result.Swapped = cmd.Operation == "cas" && write
	}
	if cmd.Operation != "latest" { next.requests[id] = record{Command: cmd, Result: out.Result, Error: out.Error} }
	return next, out
}

func kvModel() porcupine.Model {
	m := porcupine.NondeterministicModel{
		Init: func() []interface{} { return []interface{}{modelState{values: make(map[string]Result), requests: make(map[requestID]record)}} },
		Equal: func(a, b interface{}) bool { return reflect.DeepEqual(a, b) },
		Step: func(state, input, output interface{}) []interface{} {
			previous, cmd, observed := state.(modelState), input.(command), output.(historyOutput)
			if observed.Error == "not-leader" { return []interface{}{previous} }
			next, expected := modelApply(previous, cmd)
			if observed.Error == "unknown" { return []interface{}{previous, next} }
			if observed == expected { return []interface{}{next} }
			return nil
		},
		DescribeOperation: func(input, output interface{}) string { return fmt.Sprintf("%+v => %+v", input, output) },
		DescribeState: func(state interface{}) string { return fmt.Sprintf("%+v", state) },
	}
	return m.ToModel()
}

func (h *historyRecorder) check(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	operations := append([]porcupine.Operation(nil), h.ops...)
	h.mu.Unlock()
	if len(operations) == 0 { return }
	model := kvModel()
	result, info := porcupine.CheckOperationsVerbose(model, operations, 10*time.Second)
	if result == porcupine.Ok { return }
	dir := filepath.Join("..", "..", ".agent-local", "linearizability")
	if err := os.MkdirAll(dir, 0700); err != nil { t.Fatalf("history %s; visualization directory: %v", result, err) }
	file, err := os.CreateTemp(dir, strings.ReplaceAll(t.Name(), "/", "-")+"-*.html")
	if err != nil { t.Fatalf("history %s; visualization: %v", result, err) }
	path := file.Name()
	err = porcupine.Visualize(model, info, file)
	closeErr := file.Close()
	if err != nil || closeErr != nil { t.Fatalf("history %s; visualization errors: %v %v", result, err, closeErr) }
	t.Fatalf("history %s; visualization: %s", result, path)
}
