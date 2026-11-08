# RaftKV

RaftKV is a work-in-progress replicated configuration store with a Raft
implementation written in this repository. **It is not production-ready:** the
Go toolchain and module proxy were unavailable in the implementation sandbox,
so generated protobuf bindings, `go.sum`, builds, race tests, linearizability
checks and chaos tests remain unverified. See [RAFT_REVIEW.md](RAFT_REVIEW.md)
for known deviations and [BENCHMARKS.md](BENCHMARKS.md) for measurement status.

```mermaid
flowchart LR
    CLI[raftctl / Go client] -->|gRPC| API[KV service]
    API -->|commands and logged reads| R[Raft node]
    R -->|AppendEntries / votes / snapshots| Peers[Raft peers]
    R -->|committed entries| SM[KV state machine]
    SM -->|snapshot| R
    R --> Disk[(checksummed WAL and state)]
    R --> Metrics[metrics / health / tracing]
```

## Intended guarantees and limits

* Committed writes and logged Get operations are intended to be linearizable;
  CompareAndSwap compares the key's version. Versions survive deletion.
* Client ID and sequence deduplicate requests, including read results. A timeout
  is ambiguous: retry the same identity and arguments, not a new write ID.
* `ReadLatest` uses a quorum ReadIndex and does not log a read or preserve a
  retry's original result. It can return not-ready after an election until the
  leader commits a current-term entry.
* Watch uses an exclusive Raft-index cursor and bounded history; a compacted
  cursor requires a full re-read. There is currently no prefix-list API.
* Membership is fixed at startup. `member list` is read-only; dynamic add,
  remove, and learners are not implemented. Never change peer lists piecemeal.
* gRPC is plaintext and unauthenticated; use only on a trusted local network.
  Snapshot transfer is single-message and size-limited. Deduplication history
  can grow without bound. Known storage and Figure 2 risks are in the review.

## Local setup and cluster

Use Go 1.24.10 and the project's pinned tools and caches; do not install tools
globally. All temporary files, downloaded modules and toolchains must remain in
this repository. With a network-accessible module proxy and a project-local Go:

```sh
GOFLAGS="${GOFLAGS:-} -mod=mod" go tool buf generate
go mod tidy
scripts/check.sh
scripts/local-cluster.sh
```

The cluster script runs three processes on 127.0.0.1 ports 21800–21808, keeps
data in a temporary project-local directory, and removes it on exit. In a
second terminal, after generating bindings:

```sh
go run ./cmd/raftctl put app/key value
go run ./cmd/raftctl get app/key
go run ./cmd/raftctl cas app/key 1 replacement
go run ./cmd/raftctl member list
go run ./cmd/raftctl -after-revision 0 watch app/
```

`raftctl -endpoints ... -timeout ...` sets client endpoints and deadline.
Global flags precede the command. For a repeatable operation identity use both
`-client-id` and `-sequence`; callers must persist and coordinate the sequence.
Metrics and health are served on 21806–21808. `/readyz` is leader-only and
requires a current-term committed entry and a quorum-confirmed applied read.

## Verification and observability

`scripts/check.sh` generates protobuf bindings, builds, runs tests with the race
detector, vets, and runs the pinned linter. The test suite includes a seeded
in-memory network with partitions/loss/reordering, Porcupine histories, real
gRPC process tests, snapshots and restart tests. The process chaos harness is:

```sh
go run ./cmd/chaos -duration=10m -seed=20261107
```

Its results are not verified here. The optional Compose configurations and
Grafana dashboard are documented in [DESIGN.md](DESIGN.md); Docker-dependent
checks were not run because access to the host daemon is prohibited.

The benchmark-only `-benchmark-no-fsync` flag disables durability and can lose
acknowledged writes. It must never be used for real data.
