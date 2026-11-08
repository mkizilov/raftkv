# Raft Figure 2 static review

This is a source review against [Figure 2 of the Raft paper](https://raft.github.io/raft.pdf),
not a proof. Figure 2 excludes snapshots and membership changes. The baseline
build and race suite now pass with locally generated protobuf bindings, including
the short chaos smoke test; that does not establish all properties below. See
`README.md` and `ROADMAP.md` for verification status. References name the
implementation responsible for each rule.

## State and rules, line by line

| Figure 2 item | Implementation and deviations |
| --- | --- |
| Persistent `currentTerm`, `votedFor`, log before RPC replies | `nodeState.persist`, `saveLog`, `DiskStorage.save`, and `saveLog` persist before a response. MemoryStorage is intentionally volatile. `-benchmark-no-fsync` explicitly disables fsync and **does not provide the Figure 2 crash guarantee**. Disk recovery fails closed on checksum errors and discards an incomplete final record. Filesystem fsync/rename guarantees and crash behavior are not verified here. |
| Volatile `commitIndex`, `lastApplied` start at zero | `nodeState` initializes both to zero for an empty log. After snapshot recovery they start at the snapshot index, a necessary compaction extension. Commit position after the snapshot is volatile and must be relearned. |
| `lastApplied` increments when applying each committed entry | `Node.run` sends in order, but its `lastApplied` advances when the application channel accepts an entry, not when the consumer finishes applying it. The KV service separately tracks its actual applied index and calls `Snapshot` only after application. Other consumers must honor that contract; a send is not an application acknowledgement. |
| Higher term in any request or response updates term and causes follower transition | `observeTerm` persists the term, clears the vote and steps down. **Deviation:** `RequestVote` rejects an unconfigured candidate before examining its term. This is a fixed-membership guard, not Figure 2 or a dynamic-membership implementation. Other RPCs can still carry higher terms from unconfigured senders; the transport is unauthenticated. |
| Followers respond to RPCs and start an election after timeout without leader AppendEntries or a granted vote | `run`, `AppendEntries`, `RequestVote`, `elect`. **Deviation:** `observeTerm` also resets the timer for a higher-term RequestVote that is subsequently denied, and for higher-term responses. This can delay elections under repeated higher-term traffic; it is a liveness risk, not justified as a Figure 2 rule. |
| Candidate increments term, votes for itself, resets timer, sends RequestVote to all peers | `elect` persists its term and self-vote before sending. Per-node seeded pseudorandom timeouts implement randomized elections. Term exhaustion now stops the node rather than wrapping; this is a finite-representation fail-closed extension. |
| Candidate wins a majority, steps down on current-term leader AppendEntries, or restarts on timeout | `elect`, `observeTerm`, `AppendEntries`, and the timer loop. Vote responses are counted once per configured peer and only in the active term. AppendEntries in the same term makes any role, including a leader, a follower even if its predecessor later fails the consistency check; Figure 2 explicitly mentions a candidate, while same-term competing leaders should be impossible. |
| RequestVote rejects stale term and grants only one vote to an up-to-date log | `RequestVote` checks term, saved vote, then compares last term and index. Snapshot boundary supplies the compacted tail term. Unconfigured-candidate rejection above is an extra fixed-set condition. |
| Leader initializes `nextIndex` to last+1 and `matchIndex` to zero | `becomeLeader` initializes both maps; the leader counts its own log separately. Numeric index exhaustion is rejected before a next-index sentinel could wrap. |
| Leader sends initial empty heartbeats and repeats | `becomeLeader` calls `heartbeats`, and the leader timer repeats it. If a follower is behind, the first RPC can include entries rather than being empty. One in-flight RPC per follower means a delayed RPC can postpone that follower's next heartbeat until it completes or times out; this may affect liveness. |
| Client command appended locally, reply after committed application | `Node.Propose` returns an index immediately rather than being a client response. `kv.Server.execute` waits for a matching applied result before replying. Context expiry is ambiguous; retries must use the same identity. |
| AppendEntries rejects stale term and mismatching preceding index/term | `AppendEntries` checks term and predecessor. A predecessor earlier than the compacted boundary gets a `Compacted` hint, an extension outside Figure 2. |
| Delete conflicting suffix and append missing entries | `AppendEntries` replaces the suffix when index/term differs and persists it first. It refuses to overwrite an already committed index rather than proceeding: this is fail-closed under an invariant violation. It assumes Figure 2's log-matching property when index and term agree. |
| Follower advances commit to `min(leaderCommit, last new index)` | `AppendEntries` uses the predecessor plus entry count, including empty heartbeats, and never decreases commit. |
| Replicate from `nextIndex`; update progress on success; decrement on mismatch | `replicate` serializes requests per follower, copies queued entries, and now caps normal batches at 64 entries or 1 MiB. Conflict-term hints skip runs rather than decrementing one by one. An individual entry larger than the batch limit remains indivisible; if it exceeds the gRPC message limit replication can fail indefinitely. |
| Advance leader commit only for a majority and an entry in the current term | `advanceCommit` implements the current-term rule and counts the leader plus matching followers. This assumes the fixed voter set; it is not safe dynamic reconfiguration. |
| No leader no-op specified in Figure 2 | Leaders do not automatically append one. Old-term entries can wait for a later current-term command, and `ReadIndex` returns `ErrReadNotReady` until one commits. Watch explicitly appends an internal barrier when necessary. |

## Extensions and unresolved verification

* Snapshot transfer is a single RPC, not chunked, and both the stored snapshot
  record and gRPC messages have size limits. Snapshot compaction is outside
  Figure 2 and has not been verified by a crash/power-loss test here.
* Fixed voter configuration is not persisted as a configuration log entry or
  snapshot metadata. Dynamic addition, removal, learners, and safe removal of
  a leader are absent; see the membership section of `DESIGN.md`.
* The storage recovery policy treats an incomplete final WAL record as torn.
  A corrupted or truncated *first* snapshot-containing record can be discarded
  as a torn tail; distinguishing post-commit corruption from an incomplete
  append requires additional durable framing. This has not been fixed or tested.
* A 3-node, 128-client, fsync-on write benchmark exceeded its five-second
  operation deadline and showed repeated term changes without injected faults.
  Discarding canceled queued proposals reduces unnecessary work but did not
  resolve the reproduced timeout; the underlying liveness cause is unconfirmed.
* The published transport is plaintext and unauthenticated. Figure 2 assumes
  non-Byzantine peers, not hostile or misconfigured nodes.
* The baseline race and Porcupine tests pass. A ten-minute five-node chaos run
  with seed `20261107` checked 2,025 operations and returned `Ok` before the
  queued-proposal cancellation fix; the final code has only repeated the short
  smoke test. These do not prove liveness or linearizability. Extended runs
  remain separate verification work; see `ROADMAP.md`.
