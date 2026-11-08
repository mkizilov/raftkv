package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/you/raftkv/client"
)

type operation struct { Kind, Key, Value string; Expected uint64 }

type history struct {
	mu sync.Mutex
	start time.Time
	operations []porcupine.Operation
	counts map[string]int
}

func newHistory() *history { return &history{start: time.Now(), counts: make(map[string]int)} }

func (h *history) stamp() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Since(h.start).Nanoseconds()
}

func (h *history) record(id int, input operation, start, end int64, output client.Result) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.operations) >= 50000 { return fmt.Errorf("chaos: history limit exceeded; run not checked") }
	h.operations = append(h.operations, porcupine.Operation{ClientId: id, Input: input, Call: start, Return: end, Output: output})
	h.counts[input.Kind]++
	return nil
}

// Each logical operation touches one key and has a unique request identity, so
// histories can be partitioned by key without losing cross-key dependencies.
func model() porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			groups := make(map[string][]porcupine.Operation)
			for _, op := range history { key := op.Input.(operation).Key; groups[key] = append(groups[key], op) }
			result := make([][]porcupine.Operation, 0, len(groups))
			for _, group := range groups { result = append(result, group) }
			return result
		},
		Init: func() interface{} { return client.Result{} },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			current, op, observed := state.(client.Result), input.(operation), output.(client.Result)
			next, expected := current, current
			switch op.Kind {
			case "get":
			case "put":
				if current.Version == ^uint64(0) { return false, current }
				next = client.Result{Value: op.Value, Version: current.Version+1, Found: true}
				expected = next
			case "cas":
				if current.Version == op.Expected {
					if current.Version == ^uint64(0) { return false, current }
					next = client.Result{Value: op.Value, Version: current.Version+1, Found: true}
					expected = next
					expected.Swapped = true
				}
			default: return false, current
			}
			return observed == expected, next
		},
		Equal: func(a, b interface{}) bool { return a.(client.Result) == b.(client.Result) },
		DescribeOperation: func(input, output interface{}) string { return fmt.Sprintf("%+v => %+v", input, output) },
		DescribeState: func(state interface{}) string { return fmt.Sprintf("%+v", state) },
	}
}
