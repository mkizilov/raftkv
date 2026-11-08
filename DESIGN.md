# RaftKV design

This document specifies the intended implementation. Election, log replication,
file-backed storage, an in-process replicated KV API, and a gRPC client and
transport are implemented but require generated protobuf bindings. Raft will
be implemented in this repository, not with an existing Raft implementation.
The protocol below follows Figure 2 of the [Raft paper](https://raft.github.io/raft.pdf).

## Test transport

`internal/transport` defines the RPC boundary, including snapshot chunks, and a
concurrency-safe in-memory network for tests. The network can disconnect nodes,
partition groups, drop requests and responses independently, and delay each
delivery randomly, allowing concurrent calls to overtake each other. Any topology
change invalidates all in-flight RPCs, including those on unaffected links; a
handler may already have executed when its response is lost. Handlers must honor
context cancellation, and callers must use deadlines when they need bounded waits.
Each test uses a fixed seed, overridable with `RAFTKV_TEST_SEED`, and prints it
on failure. The seed reproduces random decisions for the same call order, but
cannot reproduce nondeterministic goroutine scheduling by itself. The cluster
helper uses a node adapter; election and replication tests use real nodes.
The unreliable-network regression test keeps 10% per-message loss and random
delays up to 50 ms enabled during partitions, simulated leader crashes, and
convergence. It compares applied entries at each index, including after restart;
stopping an in-process node models a crash but is not a forced process kill.
Run it and the backtracking tests repeatedly with
`go test -race -count=100 -run 'TestUnreliablePartitionsAndLeaderCrashes|TestFastBacktrackingThousands|TestBacktrackHints' ./internal/raft`.

## gRPC transport and server

`proto/raftkv/v1/raftkv.proto` defines the Raft RPCs and implemented KV calls.
Generate the Go bindings under `gen/` with `go tool buf generate`; the Buf
template runs the pinned `protoc-gen-go` and `protoc-gen-go-grpc` through
`go tool`. `scripts/check.sh` generates bindings before building and testing.
The bindings and `go.sum` have not been generated in this checkout: the local
Go toolchain is unavailable and the module proxy could not be resolved. Install
the pinned toolchain and download the pinned modules inside this repository,
generate the bindings, and resolve checksums before building or running tests.
The runtime uses one reusable connection per peer, inherits caller deadlines,
and applies an additional RPC timeout. Raft and KV listen on separate addresses.

`raftkv-server` takes `-id`, `-raft-listen`, `-client-listen`, `-data-dir`, and
`-peers`, where each peer is `id=raft-address@client-address` and the list
includes the local node. `-snapshot-bytes` sets the KV snapshot threshold.
Follower client requests return gRPC `FailedPrecondition` with a
`NotLeaderDetail` containing the last known leader's client address; the
address is empty before a leader is known and is only a best-effort hint during
partitions. Clients must retry safely with the same identity and sequence.
`scripts/local-cluster.sh` builds and runs three loopback nodes on ports
21800–21805, removes its temporary data on exit, and stops all three on Ctrl-C.
The integration test binds loopback ports 21860–21868.

This transport currently uses plaintext, unauthenticated gRPC and is intended
only for trusted local testing; do not expose it to untrusted networks. TLS and
membership changes are not yet implemented.

## Client and CLI

The `client` package reuses one gRPC connection per configured endpoint. It
follows not-leader hints only if the advertised address is in the configured
endpoint list, otherwise rotates through configured nodes. Transient unavailable
and per-attempt deadline errors are retried with bounded exponential backoff
and jitter until the caller's context expires. Each operation reserves a client
ID and sequence once and reuses them across all internal attempts. A context
deadline or cancellation is ambiguous: a write may have committed despite the
error. Callers that need to retry after the method returns must reserve and
persist a `RequestID` with `NewRequest`, then retry the identical operation via
its `WithID` method. Reusing a client ID across processes requires an externally
persisted, coordinated sequence counter; the default is a fresh random ID.

`raftctl` accepts global flags before its command: `-endpoints`, `-timeout`,
and optionally `-client-id` with `-sequence` for a repeatable request identity.
Commands are `get key`, `put key value`, `delete key`, `cas key version value`,
`status`, `bench count key`, and `watch prefix`. Status reports each endpoint's role, leader,
term, and commit index, including per-node errors. Bench performs actual logged
reads with two workers and reports completed operations and measured throughput.
The process-failure integration test builds a race-enabled server, binds
loopback ports 21880–21888, kills the leader, and checks the continuing workload;
it requires the local Go toolchain and generated bindings.

## Process chaos harness

`go run ./cmd/chaos -duration=10m -seed=20261107` builds a race-enabled server
inside a temporary project-local directory and starts five real node processes.
Each node has a client proxy and a separate directed TCP proxy for every other
node, all on loopback ports 21800–21844 including telemetry. Directed links allow a 2/3 partition
without relying on host firewalls or Docker. Partitions close existing proxied
connections as well as rejecting new ones; latency delays traffic in both
directions. A seeded schedule kills and restarts nodes with their data intact,
partitions links, injects latency, and freezes/resumes child processes using
SIGSTOP/SIGCONT. Seeded choices are reproducible, but process scheduling and
TCP timing are not.

Two concurrent clients issue unique logical Put, Get and CAS operations on
two keys, and record invocation/return times and results across client retries.
When fault injection ends, links are healed and in-flight calls get up to 30
seconds to drain. An incomplete/ambiguous operation fails the run; it is never
silently omitted from a passing history. Porcupine partitions by key and checks
the versioned model, with a 60-second checker timeout treated as failure. History
is capped at 50,000 completed operations and reaching the cap fails the run;
the default 300 ms inter-operation delay keeps the ten-minute workload bounded.
The summary includes the seed, fault counts, operation counts and checker
verdict. A failing or timed-out check writes an HTML visualization under
`.agent-local/chaos-failures/`. Owned processes, proxy listeners and temporary
data are cleaned up on exit; if a process cannot be stopped, its exact PID and
the preserved temporary directory are reported.

`docker-compose.yml` runs the same five child processes and proxies inside one
isolated container, with no Docker socket or host ports mounted. The image is
pinned by digest and uses project-local caches; it is a reproducible variant,
not a five-container distributed deployment. Docker execution is unverified in
this sandbox because host Docker access is forbidden. The required ten-minute
native chaos run also remains unverified until the local Go toolchain and
generated protobuf bindings are available.

## Observability

`raftkv-server -http-listen 127.0.0.1:21890` exposes `/metrics`, `/healthz`,
and `/readyz`; the local cluster uses 21806–21808 instead. `/healthz` reports
whether the server and Raft owner are running. `/readyz` is leader-only: it
performs a ReadIndex quorum round and waits for the KV apply loop. A leader that
has not yet committed an entry in its current term, an isolated leader, or a
follower is not ready. Liveness alone does not prove quorum or progress.

Prometheus metrics include current term, one-hot role, commit and KV-applied
indexes, distinct observed leader/term pairs (including the first observed
leader), current snapshot payload size, and gRPC request counts and duration
histograms by method and status. Watch latency covers the lifetime of a stream.
The process owns a separate registry per node; request IDs are not metric labels.

Server logs are JSON via `log/slog`; `-log-level` accepts debug, info, warn,
or error. Every emitted application log includes node ID, current observed term,
role, and request ID (`-` outside a request); trace IDs are included when valid.
The client propagates W3C trace context and a stable hashed request ID across
retry attempts. Server spans include Raft proposal and committed/applied events;
an application using the client can install an OpenTelemetry tracer provider
to create upstream spans. Set `-otlp-endpoint` to a full OTLP/HTTP traces URL
such as `http://127.0.0.1:21892/v1/traces` to export server spans. No spans are
exported by default; exporter shutdown is bounded. Do not put credentials in
the endpoint URL or expose the unauthenticated HTTP or gRPC listeners to an
untrusted network.

The Compose `observability` profile provisions Prometheus, Grafana, a three-node
cluster, a datasource, and `deploy/grafana/dashboards/raftkv.json`. Use
`docker compose --profile observability up telemetry-cluster prometheus grafana`.
These services share a private network namespace and bind loopback only; no host
ports or Docker socket are exposed. Prometheus listens on 21890 and Grafana on
21891 inside that namespace, with anonymous viewer access. An operator needs a
separately secured access path to view Grafana from outside the namespace.
The profile and OTLP export have not been run in this sandbox.

## Election implementation and concurrency

Each node has one goroutine owning its term, vote, role, election timer, and
vote tally. RPC handlers and status queries submit events to that goroutine;
their buffered replies cannot block the owner if a caller cancels. Outbound
RPCs run in separate, deadline-bound goroutines and return results as events.
The owner does not hold locks across I/O or channel sends. Shutdown cancels RPCs
and waits for the owner and RPC workers. The storage interface atomically saves
term and vote before a vote or higher term is reported; a storage error stops
the node rather than allowing an unsafe response. The provided memory storage
survives only while its instance remains alive and is not durable across a
process restart. Nodes must be registered with the transport before Run.

Election timeouts default to a random duration in [150 ms, 300 ms), with 50 ms
heartbeats; all are configurable. Each node's seeded RNG is mixed with its ID.
The vote freshness comparison uses the persisted local log tail. A leader
isolated from the majority remains a leader in its old term until it observes a
higher term, so clients must not treat its role alone as a linearizable lease.

## Replication implementation

`Start(command)` appends a copied command on the leader and returns its index,
term, and leadership status without waiting for commit; `Propose` additionally
accepts a context and reports errors. Application-level client responses must
wait for commitment and application. Each leader has at most one AppendEntries
RPC in flight per follower; failures retry on the next heartbeat. For an
inconsistent previous entry, the follower returns the conflicting term and its
first index, or its log length plus one if the previous index is missing. The
leader jumps to the index after its own last entry in that term, or to the
follower's conflict index when the term is absent. Hints that cannot make
backward progress fall back to decrementing `nextIndex`. Current-term
entries advance the leader's commit index only after a majority replicates them;
earlier entries become committed indirectly. Followers persist log changes
before acknowledging them and never overwrite an already committed entry.
Queued log entries are grouped into batches of at most 64 entries or about
1 MiB per AppendEntries RPC. An oversized single entry is sent intact; it may
still exceed the gRPC message limit. Local proposal persistence remains per entry.

The state owner sends copied committed entries in index order on the configured
apply channel using a selectable send, so a slow or absent consumer does not
block elections, RPCs, or shutdown. An absent consumer prevents delivery, not
commit. Commit and application indexes are volatile: after a restart the owner
relearns commitment from a leader and may deliver entries again. Consumers must
account for replay until a durable state-machine recovery protocol is added.
Storage log replacement is atomic at the interface boundary; the in-memory
implementation is not durable across process restarts.

## Snapshots

After applying an entry, the service may call `Snapshot(index, data)` with its
complete state through that index. Raft persists the image, its index and term,
and the retained suffix atomically before discarding the prefix in memory.
Indexes remain absolute; the snapshot boundary supplies the preceding term for
AppendEntries and voting. A leader sends InstallSnapshot when a follower's next
index falls behind that boundary, then resumes normal suffix replication.
InstallSnapshot currently sends a complete image in one RPC; nonzero offsets and
partial chunks are rejected. A snapshot and its retained suffix must fit the
storage record limit described below.

On restart or installation, Raft sends an `ApplyMsg` with `SnapshotValid`,
snapshot index, term and bytes before sending later committed log entries.
The service must restore that image before processing subsequent messages and
must create a snapshot only after applying its index. An image may be replayed
after restart; commit and application positions after the snapshot are volatile.

## KV state machine

`internal/kv.Server` owns a Raft node and its apply stream. Register its node
with the transport, then start the server. `Get`, `Put`, `Delete`, and
`CompareAndSwap` all enter the Raft log, including reads; a response is returned
only after the matching request has been committed and applied. Context expiry
is ambiguous: the command may still commit, so retry with the same identity and
arguments. A disconnected former leader cannot serve a new local read without
committing it. The same operations are exposed through the gRPC client API.

Every request includes a nonempty client ID and a positive, increasing sequence
number. The replicated state stores the original command and result for every
client/sequence pair, including reads and failed CAS operations. A retry returns
the original result without executing again; reuse with different arguments is
an error. Entries are retained indefinitely, which supports delayed retries but
requires a future client-session garbage-collection policy to bound memory.
Clients must persist their IDs and sequence numbers and must not reuse a pair for
another operation. Keys start at version zero. Put and Delete always increment
the version, including deletion of an absent key; tombstones retain versions to
prevent ABA on recreation. CompareAndSwap writes only when the current version
matches, including a tombstone's version, and increments on success. A version
overflow is rejected deterministically.

The KV snapshot contains values, tombstone versions, all deduplication records,
and the applied index. When the retained Raft log exceeds the configured
threshold of logical command bytes plus eight bytes per term, the service
snapshots after applying an entry. Zero disables automatic snapshots. Restoring
the image precedes subsequent apply messages, preserving deduplication through
leader changes and restarts. Snapshot size can still grow with retained client
history and is subject to Raft's record-size limit.

## Watch

`Watch(prefix, afterRevision)` streams committed changes for keys with that
prefix, in Raft log order. Revisions are global log indexes, so gaps are normal;
the cursor is exclusive and zero requests all retained events. Each new Put,
Delete (including an absent-key tombstone), or successful CAS emits one event;
retries, reads and failed CAS operations do not. Events carry revision, key,
value, version and deletion status. Up to 128 change events and at most 1 MiB
of event payload plus per-event accounting are retained. An event larger than
the byte budget compacts history through that event, requiring affected watchers
to re-read. The retained events and revision through which history was evicted are part of the replicated state-machine
snapshot. A legacy snapshot without history marks its whole prefix compacted.

Before starting, the leader establishes a ReadIndex barrier; if its term has no
committed entry, the service proposes an internal no-op barrier. The apply loop
must reach that index before events are served. The stream periodically confirms
leadership with a fresh quorum round and ends on term change or quorum loss. The
client reconnects and resumes after the last event successfully passed to its
callback. It must persist that revision itself for process restarts. A slow or
disconnected consumer whose cursor precedes the compacted revision receives an
`OUT_OF_RANGE` error with `WatchCompactedDetail` and must re-read full state;
the present API has no prefix-list operation, so applications must obtain a
full key list from another source. Filters may conservatively report compaction
when only unrelated events were evicted. A blocked gRPC send applies backpressure
without holding Raft or state-machine locks.

`raftctl [-after-revision N] watch prefix` prints one JSON event per line and
runs until interrupted by default. An explicit `-timeout` bounds it. Exactly-once
delivery across a callback failure or process crash requires the application to
persist the event and its cursor atomically; the stream itself is resumable, not
an exactly-once delivery mechanism.

## Disk storage and recovery

`OpenDiskStorage` requires an existing, exclusively owned directory; concurrent
instances or processes using the same directory are unsupported. A dedicated
storage goroutine serializes operations without holding a lock during file I/O;
call `Close` after stopping the node to release its file and goroutine. The state file
contains a version, term, vote and checksum. A replacement is written to a temp
file in that directory, synced, renamed over the state file, and followed by a
directory sync. Log changes append a versioned, checksummed record describing
the retained prefix and replacement suffix, then sync the log before returning.
The log file is created and its directory synced on open. The explicitly unsafe
`-benchmark-no-fsync` flag disables file and directory synchronization for
measurements only and can lose acknowledged data. On snapshot, a new
log file containing the image and suffix is synced, atomically renamed over the
old file, and followed by a directory sync; subsequent mutations append to it.
This bounds retained disk log history when the service snapshots regularly.
Each encoded mutation, including an image and suffix, is limited to 64 MiB.

Recovery replays complete records and truncates and syncs an incomplete final
header or payload. A complete record with an invalid header or checksum, an
invalid prefix, or a corrupt state file causes an error rather than silently
discarding potentially acknowledged data. A write or sync failure poisons the
storage instance; the node stops and must be reopened to recover its on-disk
state. A crash during replacement may leave a `.state-*` or `.log-*` temp file;
neither is used for recovery. Successful saves remove their temporary file. Commit
and apply positions remain volatile and a recovered state machine must tolerate
replay; disk storage alone does not provide application-level exactly-once
execution. File sync and rename durability assume a filesystem honoring those
operations; no host-specific power-loss guarantees are claimed.
The restart regression test stops and recreates nodes at seeded points during
replication; this models node restarts but is not an operating-system kill or
power-loss test. A separate subprocess exits without closing storage after
synced writes, and torn-record tests inject truncated bytes explicitly.

## Raft state

Each server persists `currentTerm` (initially 0), `votedFor` (initially none),
and a log of entries containing a command and its term. Changes to persistent
state must reach stable storage before responding to the relevant RPC. Each
server also keeps volatile `commitIndex` and `lastApplied` (initially 0).
Leaders initialize volatile `nextIndex` for each follower to the leader's last
log index plus one and `matchIndex` to 0. Index 0 denotes the empty prefix.

## Membership safety and current limitation

Membership is currently fixed by each node's startup configuration. Status
and `raftctl member list` expose those configured voter IDs, and a node does
not grant votes or advance its term for a candidate outside that fixed set.
This is not dynamic membership: `member add` and `member remove`, non-voting
learners, configuration log entries, and membership recovery from snapshots
are not implemented. All nodes must be started with the same fixed voter set.
Do not attempt to change it by restarting individual servers with new flags.

The proposed single-server change protocol requires the latest configuration
in each server's log to govern elections and quorum decisions even before its
entry commits; truncation must restore the preceding configuration. A new
configuration may differ by at most one voter, and the previous change must be
committed before another begins. A prospective voter must first catch up as a
non-voting learner. Before proposing any change, a leader must also commit an
entry from its current term: the original thesis algorithm without that guard
has a published split-brain/data-loss counterexample for concurrent changes
across terms. See [Ongaro's correction](https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E)
and the [thesis](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf).
Correct implementation also requires durable configuration metadata in snapshots,
dynamic transport addresses, a removed leader stepping down after commitment,
and quorum-safe reads. None of these are supplied by the fixed-set status view.

## RPCs

**RequestVote** carries `term`, `candidateId`, `lastLogIndex`, and
`lastLogTerm`; the reply carries the receiver's `term` and `voteGranted`.
Reject a stale term. Grant a vote only if the server has not voted in the term
or has already voted for that candidate, and the candidate's log is at least
as up to date as the receiver's: compare last terms first, then last indices.

**AppendEntries** carries `term`, `leaderId`, `prevLogIndex`, `prevLogTerm`,
new `entries` (possibly empty for a heartbeat), and `leaderCommit`; the reply
carries the receiver's `term` and `success`. Reject a stale term, or a request
whose previous index and term do not match the local log. If a new entry
conflicts with an existing entry at the same index but a different term, delete
that entry and everything following it; append entries not already present.
Advance `commitIndex` to the minimum of `leaderCommit` and the index of the
last new entry when `leaderCommit` is greater.

## Server rules

* All servers apply entries in order while `commitIndex > lastApplied`,
  incrementing `lastApplied` for each entry. On any request or response with a
  higher term, update `currentTerm`, clear `votedFor`, and become a follower.
* Followers respond to RPCs. If an election timeout elapses without receiving
  AppendEntries from the current leader or granting a vote, become a candidate.
* Candidates start an election by incrementing the term, voting for themselves,
  resetting the election timer, and sending RequestVote RPCs to all other
  servers. Win with votes from a majority in the same term; become a follower
  on valid AppendEntries from a leader; start another election on timeout.
* Leaders send empty AppendEntries to all followers as heartbeats and retry
  replication. Append client commands to the log and reply only after the
  entry has been committed and applied. If a follower is behind, send entries
  starting at its `nextIndex`, including the preceding index and term. On
  success update its `nextIndex` and `matchIndex`; on a consistency failure
  use the conflict hint to backtrack and retry. Advance `commitIndex` to an index N greater
  than the current commit index when a majority have `matchIndex >= N` and
  the entry at N is from the current term.

## Client API and consistency

* `Get(key)` returns the current value or a not-found result.
* `Put(key, value)` stores a value; `Delete(key)` removes a key.
* `CompareAndSwap(key, expectedVersion, replacement)` atomically replaces a value
  only when its current version matches, and reports success or failure.
* `Watch(prefix, afterRevision)` streams retained and future committed changes
  after an exclusive revision, or reports that history was compacted.

Reads and writes are intended to be linearizable: each completed operation
appears to take effect at a single point between invocation and response,
respecting real-time ordering. Writes, including compare-and-swap decisions,
are ordered through the replicated log. Reads also commit through the log; a
leader's local state alone is insufficient.
Watch events reflect applied committed log order; this does not by itself make a
watch stream a linearizable read. Failure and retry semantics remain to be
specified with the implementation.

## ReadIndex and history checking

`raft.Node.ReadIndex` requires a committed entry from the current leader's term,
then sends a fresh AppendEntries heartbeat round and waits for a same-term
majority. A follower's log-consistency rejection still acknowledges the term;
the probe carries no entries or commit advancement. It returns the commit index
captured before the round, and callers must wait until their state machine has
applied at least that index. A new leader without a committed current-term entry
returns `ErrReadNotReady`; this implementation does not automatically append a
no-op. Context cancellation aborts the round.

`Server.ReadLatest` uses this barrier, waits for the KV apply loop, and serves
a fresh read without appending a log entry; it is also exposed as an opt-in
gRPC and Go-client method for benchmarking. It has no client request identity:
each retry is a new read and can return a newer value. The existing `Get` remains
log-based and preserves its replicated exactly-once read-result cache. Replacing
it outright with a read-only quorum round would lose the original result of a
retried read after a leader change; replicated result storage or an explicit API
contract change is required before that replacement is safe.

KV test histories record invocation and return times, arguments and outcomes and
are checked against a versioned, deduplicating Porcupine model. Concurrent fault
tests record each logical request across its retries. A timed-out operation is
modeled as either not executed or executed by its return; effects occurring
later can make this conservative model reject a valid history. Checker timeouts
are failures, not passes. A failing check writes an HTML visualization under
`.agent-local/linearizability/`. `go test -race -run '^$' -bench BenchmarkReads
./internal/kv` compares logged Get with ReadLatest; they have different retry
contracts and no throughput result is assumed without running the benchmark.
