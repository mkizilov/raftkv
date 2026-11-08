#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
mkdir -p .agent-local/tmp
work=$(mktemp -d "$(pwd)/.agent-local/tmp/raftkv-cluster.XXXXXX")
pids=""
cleanup() {
  for pid in $pids; do kill "$pid" 2>/dev/null || :; done
  for pid in $pids; do wait "$pid" 2>/dev/null || :; done
  rm -r "$work"
}
trap cleanup EXIT
trap 'exit 0' INT TERM

go tool buf generate
go build -o "$work/raftkv-server" ./cmd/raftkv-server
peers='1=127.0.0.1:21800@127.0.0.1:21801,2=127.0.0.1:21802@127.0.0.1:21803,3=127.0.0.1:21804@127.0.0.1:21805'
for id in 1 2 3; do
  raft_port=$((21800 + (id - 1) * 2))
  client_port=$((raft_port + 1))
  "$work/raftkv-server" -id "$id" -raft-listen "127.0.0.1:$raft_port" -client-listen "127.0.0.1:$client_port" -peers "$peers" -data-dir "$work/node-$id" &
  pids="$pids $!"
done
printf '%s\n' 'Client endpoints: 127.0.0.1:21801, 127.0.0.1:21803, 127.0.0.1:21805 (Ctrl-C to stop; data is temporary).'
while :; do
  for pid in $pids; do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || :
      printf '%s\n' 'A node exited; stopping the cluster.' >&2
      exit 1
    fi
  done
  sleep 1
done
