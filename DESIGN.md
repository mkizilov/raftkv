# RaftKV design

This document specifies the intended implementation. Election, log replication,
file-backed storage, and an in-process replicated KV API exist, but production
networking and a network client API do not yet. Raft will
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
committing it. This is an in-process API; transport-facing client RPCs are not
yet implemented.

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

## Disk storage and recovery

`OpenDiskStorage` requires an existing, exclusively owned directory; concurrent
instances or processes using the same directory are unsupported. A dedicated
storage goroutine serializes operations without holding a lock during file I/O;
call `Close` after stopping the node to release its file and goroutine. The state file
contains a version, term, vote and checksum. A replacement is written to a temp
file in that directory, synced, renamed over the state file, and followed by a
directory sync. Log changes append a versioned, checksummed record describing
the retained prefix and replacement suffix, then sync the log before returning.
The log file is created and its directory synced on open. On snapshot, a new
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
* `Watch` streams committed changes to selected keys; precise selection, resume,
  and backpressure semantics will be specified when the API is implemented.

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
a fresh read without appending a log entry. It has no client request identity:
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
