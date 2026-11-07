# RaftKV design

This document specifies the intended implementation. This initial scaffold does
not yet implement consensus, storage, networking, or the client API. Raft will
be implemented in this repository, not with an existing Raft implementation.
The protocol below follows Figure 2 of the [Raft paper](https://raft.github.io/raft.pdf).

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
  decrement `nextIndex` and retry. Advance `commitIndex` to an index N greater
  than the current commit index when a majority have `matchIndex >= N` and
  the entry at N is from the current term.

## Planned client API and consistency

* `Get(key)` returns the current value or a not-found result.
* `Put(key, value)` stores a value; `Delete(key)` removes a key.
* `CompareAndSwap(key, expected, replacement)` atomically replaces a value only
  when the current value matches the expectation, and reports success or failure.
* `Watch` streams committed changes to selected keys; precise selection, resume,
  and backpressure semantics will be specified when the API is implemented.

Reads and writes are intended to be linearizable: each completed operation
appears to take effect at a single point between invocation and response,
respecting real-time ordering. Writes, including compare-and-swap decisions,
are ordered through the replicated log. Reads must confirm current leadership
with a quorum and wait until the relevant committed state is applied, or use an
equivalent Raft-safe mechanism; a leader's local state alone is insufficient.
Watch events reflect applied committed log order; this does not by itself make a
watch stream a linearizable read. Failure and retry semantics remain to be
specified with the implementation.
