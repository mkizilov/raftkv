package main

import (
	"io"
	"testing"

	"github.com/you/raftkv/internal/transport"
)

func TestParseConfig(t *testing.T) {
	peers := "1=127.0.0.1:21800@127.0.0.1:21801,2=127.0.0.1:21802@127.0.0.1:21803"
	c, err := parseConfig([]string{"-id=1", "-data-dir=data", "-peers="+peers}, io.Discard)
	if err != nil || c.id != 1 || c.raftPeers[transport.NodeID(2)] != "127.0.0.1:21802" || c.clientPeers[transport.NodeID(2)] != "127.0.0.1:21803" { t.Fatalf("config %+v, %v", c, err) }
	for _, value := range []string{"", "1=bad", "1=localhost:21800@localhost:21801,1=localhost:21802@localhost:21803", "2=localhost:21802@localhost:21803"} {
		if _, err := parseConfig([]string{"-id=1", "-data-dir=data", "-peers="+value}, io.Discard); err == nil { t.Fatalf("accepted %q", value) }
	}
	if _, err := parseConfig([]string{"-id=1", "-data-dir=data", "-peers="+peers, "-log-level=verbose"}, io.Discard); err == nil { t.Fatal("accepted invalid log level") }
	bench, err := parseConfig([]string{"-id=1", "-data-dir=data", "-peers="+peers, "-benchmark-no-fsync"}, io.Discard)
	if err != nil || !bench.benchmarkNoFsync || c.benchmarkNoFsync { t.Fatalf("benchmark fsync flag: %+v %v", bench, err) }
}
