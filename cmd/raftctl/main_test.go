package main

import (
	"bytes"
	"testing"
)

func TestRun(t *testing.T) {
	var output bytes.Buffer
	run(&output)
	if got, want := output.String(), "raftctl: client not yet implemented\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}
