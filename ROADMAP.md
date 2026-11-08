# RaftKV roadmap

This roadmap is ordered by safety and unmet acceptance criteria before feature
work. Estimates are engineering hours for one experienced Go/Raft engineer;
they are ranges, not commitments. They assume a project-local Go 1.24.10
toolchain, a reachable Go module proxy, generated protobuf bindings, two normal
build/test workers, and disposable loopback test environments. They exclude
external security audit, extended outage investigation, production rollout,
and physical power-failure testing. Steps can overlap in elapsed time, but the
total below sums effort once per step. Findings from the first successful test
runs could materially increase it.

## Current evidence and status

* **Implemented in source, unverified:** fixed-membership election and
  replication, disk storage and snapshots, KV/deduplication, gRPC server and
  client, Watch, observability, chaos and benchmark harnesses. Source presence
  is not evidence that these build, pass tests, or meet their guarantees.
* **Partially implemented:** `ReadLatest` uses ReadIndex, but `Get` remains
  log-based to preserve retry-result semantics; membership has read-only fixed
  voter listing and no dynamic changes; Watch has no full-prefix listing API.
* **Not executed:** generated protobuf bindings, `go.sum`, Go build/race/vet/
  lint, Porcupine histories, real gRPC/process integration, the required
  ten-minute chaos pass, and the 3/5-node benchmark matrix. Docker/Compose and
  OTLP export were not run because host Docker access is prohibited here.
* **Verified here:** shell scripts parse, Compose/provisioning YAML and the
  Grafana dashboard JSON parse, staged whitespace checks pass, and targeted
  tracked-file secret-pattern searches found no matches. These are narrow
  checks, not a security audit. No hardware is part of this repository; real
  power-loss behavior has not been tested.

## 1. Restore reproducible local builds and execute the baseline — P0

* **Scope:** Install the pinned Go toolchain inside the repository, resolve the
  pinned modules, generate the protobuf bindings and `go.sum`, and run
  `scripts/check.sh`; repair compilation, lint and race failures found.
* **Rationale:** No Go build or test has run, and the checkout imports bindings
  that are currently absent.
* **Dependencies:** Network-accessible module proxy and project-local toolchain;
  preserve the existing cache and sandbox boundaries.
* **Acceptance:** A clean checkout generates reproducibly, has intended
  checksums/bindings, and passes the complete check script with `-race`.
* **Effort:** 12–20 hours.

## 2. Eliminate snapshot/WAL recovery ambiguity — P0

* **Scope:** Redesign durable framing or a crash-safe manifest so a truncated
  first snapshot-containing record cannot be mistaken for an empty/torn WAL.
* **Rationale:** The current recovery path can discard a truncated snapshot
  record; see `RAFT_REVIEW.md`. That risks losing acknowledged state.
* **Dependencies:** Step 1; retain compatibility or supply an explicit migration.
* **Acceptance:** Fault injection at each write, sync and rename boundary
  recovers the last acknowledged snapshot/log or fails closed, never silently
  starts empty; corruption and partial-header regressions pass with `-race`.
* **Effort:** 16–28 hours.

## 3. Verify and correct Figure 2 election and replication behavior — P0

* **Scope:** Exercise election timers, term/vote persistence, batching,
  backtracking and current-term commit rules; resolve the timer-reset and
  fixed-voter deviations identified in `RAFT_REVIEW.md`.
* **Rationale:** Consensus behavior is untested, and repeated denied higher-term
  votes can delay election progress.
* **Dependencies:** Steps 1–2.
* **Acceptance:** Figure 2 deviations are fixed or justified with regression
  tests; 3/5-node election tests pass `-race -count=50`, and the unreliable
  network/backtracking tests pass `-race -count=100` without relaxed assertions.
* **Effort:** 16–28 hours.

## 4. Bound and validate replicated message sizes — P0

* **Scope:** Enforce compatible application-entry, AppendEntries, protobuf,
  WAL and gRPC limits; reject impossible requests before they enter the log.
* **Rationale:** A single oversized entry currently bypasses the batch byte
  target and can exceed transport limits, preventing follower catch-up.
* **Dependencies:** Steps 1 and 3.
* **Acceptance:** Boundary and oversized-entry tests demonstrate explicit
  errors, bounded allocations and continued replication for valid entries.
* **Effort:** 10–18 hours.

## 5. Prove restart and durability behavior — P0

* **Scope:** Run and extend node/whole-cluster restart tests using real child
  processes, abrupt kills and injected torn writes around snapshots and logs.
* **Rationale:** Existing node restart tests mostly model stops; process exit
  and storage behavior have not been executed or power-loss tested.
* **Dependencies:** Steps 1–4.
* **Acceptance:** Seeded 3/5-node crash/restart suites preserve every observed
  committed entry and converge; controlled power-loss tests, if required, have
  a separate disposable-environment checklist and recorded evidence.
* **Effort:** 14–24 hours.

## 6. Harden linearizability histories and the checker model — P0

* **Scope:** Model ambiguous timeouts and later commits faithfully, verify
  operation IDs, ensure each KV history is captured, and retain failure HTML.
* **Rationale:** The existing timeout model is conservative and no Porcupine
  history has run; false positives and missing observations need review.
* **Dependencies:** Steps 1, 3 and 5.
* **Acceptance:** Known legal and illegal histories, delayed-commit retries,
  CAS conflicts and partitions are checked correctly; checker timeouts fail
  explicitly and every relevant test records and checks its history.
* **Effort:** 16–28 hours.

## 7. Secure peer and client transport — P0

* **Scope:** Add mTLS, peer identity authorization, certificate rotation and
  safe error/log handling for the Raft and client gRPC listeners.
* **Rationale:** Current gRPC and metrics endpoints are unauthenticated; they
  must not be exposed to an untrusted network.
* **Dependencies:** Step 1 and a project-local test CA; no host credentials.
* **Acceptance:** Tests reject unauthenticated/wrong-node peers, exercise
  rotation and expired credentials, and keep generated test keys untracked.
* **Effort:** 20–36 hours.

## 8. Verify gRPC, CLI, and leader failover end to end — P0

* **Scope:** Generate and check bindings, run the three-node gRPC integration,
  CLI commands, status/leader hints and actual leader-process kill tests.
* **Rationale:** Network code and retry behavior exist only as unexecuted source.
* **Dependencies:** Steps 1 and 3; use Step 7 for any non-loopback deployment.
* **Acceptance:** A fresh local cluster starts and cleans up; writes, reads,
  redirects, retries and CLI status pass `-race`, including response-loss and
  leader-kill cases without duplicate writes.
* **Effort:** 20–36 hours.

## 9. Complete watch reconciliation and failover — P1

* **Scope:** Add a linearizable full-prefix listing/revision API, then test
  watch resume, retention compaction, snapshots, filtering and slow consumers.
* **Rationale:** Watch asks a lagging client to re-read full state, but the
  current API cannot list all keys under a prefix; streaming is unverified.
* **Dependencies:** Steps 1, 3, 6 and 8.
* **Acceptance:** A client can obtain a consistent prefix snapshot and cursor,
  reconnect across leader changes without missed events, and receive an
  actionable compaction error when its cursor is outside retained history.
* **Effort:** 16–28 hours.

## 10. Resolve ReadIndex and exactly-once read semantics — P1

* **Scope:** Design and implement a read contract that reconciles fast reads,
  retries and leader changes; handle a new leader's current-term barrier.
* **Rationale:** `ReadLatest` is separate and non-deduplicated; the requested
  replacement of log-based `Get` is incomplete, and a pure quorum read cannot
  recover a prior read result across leadership changes by itself.
* **Dependencies:** Steps 3, 6 and 8; explicitly document any API contract change.
* **Acceptance:** Concurrent histories and retry-after-failover tests satisfy
  the chosen documented contract, with no stale reads or silently lost retry
  semantics; distinguish comparable and non-comparable performance results.
* **Effort:** 16–32 hours.

## 11. Execute and repair the ten-minute chaos acceptance — P1

* **Scope:** Run the real five-process/proxy harness for ten minutes with
  partitions, SIGKILL/restarts, latency and SIGSTOP/SIGCONT; fix each failure.
* **Rationale:** The harness exists, but the required zero-violation run never
  started and no chaos-derived regression has been validated.
* **Dependencies:** Steps 1–6 and 8; no host Docker socket is needed.
* **Acceptance:** A recorded ten-minute run checks the complete history with
  Porcupine and passes; every discovered defect has a focused regression test.
* **Effort:** 16–32 hours, excluding unpredictable debugging beyond the range.

## 12. Implement safe single-server membership changes — P1

* **Scope:** Add non-voting learners, catch-up, one-at-a-time configuration log
  entries, dynamic transport addresses, rollback and snapshot persistence,
  quorum-safe reads, and `member add/remove/list`.
* **Rationale:** The stretch membership requirement is unimplemented. Static
  startup peer lists cannot safely be edited one node at a time.
* **Dependencies:** Steps 1–6 and 8; implement Ongaro's current-term-commit
  correction before each transition and use the latest log configuration.
* **Acceptance:** 3→5→3 under load, concurrent proposals, uncommitted-change
  rollback, restarts/snapshots and current-leader removal pass race and
  linearizability tests; no overlapping unsafe configuration changes.
* **Effort:** 64–120 hours.

## 13. Run and publish the benchmark matrix — P1

* **Scope:** Run the bounded 3/5-node × 1/16/128-client × fsync on/off matrix
  for Put, logged Get and ReadIndex, recording throughput and p50/p99.
* **Rationale:** `BENCHMARKS.md` contains no measured results; the benchmark
  runner, batching behavior and unsafe fsync-off mode are unverified.
* **Dependencies:** Steps 1, 3, 4 and 8; use disposable local datasets only.
* **Acceptance:** Publish raw repeated results, host/filesystem and methodology,
  explain observed effects, distinguish read contracts, and clean all datasets.
* **Effort:** 12–24 hours, excluding benchmark wall-clock time.

## 14. Verify observability and deployment assets — P1

* **Scope:** Exercise logs, metrics, health/readiness, W3C traces through
  commit, OTLP export, dashboard queries and Compose provisioning.
* **Rationale:** Source and configuration exist, but only YAML/JSON syntax was
  checked; Docker and trace export have not run.
* **Dependencies:** Steps 1 and 8; use an explicitly authorized disposable
  Docker environment, never the host daemon from this sandbox.
* **Acceptance:** Integration evidence confirms metric values, trace linkage,
  structured fields, endpoint behavior, dashboard provisioning and cleanup.
* **Effort:** 8–16 hours.

## 15. Bound exactly-once session retention — P2

* **Scope:** Introduce explicit client sessions, acknowledgement/watermark or
  lease semantics, and safe deduplication-record garbage collection.
* **Rationale:** Every request result is retained indefinitely and snapshots
  can exceed the 64 MiB record limit.
* **Dependencies:** Steps 5, 6 and 8; preserve documented retry behavior.
* **Acceptance:** Long-lived workloads keep bounded state, delayed retries have
  explicit outcomes, and restart/failover tests prove no duplicate mutations.
* **Effort:** 16–32 hours.

## 16. Stream and optimize snapshots and persistence — P2

* **Scope:** Add chunked snapshot transfer, bounded buffering, cancellation,
  and measure WAL/snapshot throughput before targeted optimization.
* **Rationale:** Snapshot transfer is currently a single size-limited RPC and
  local proposal persistence is per-entry even when replication is batched.
* **Dependencies:** Steps 2, 4, 5 and 13.
* **Acceptance:** Large snapshots recover across disconnect/retry with bounded
  memory, and measured changes retain the fsync-on crash guarantees.
* **Effort:** 20–36 hours.

## 17. Establish CI and operational recovery procedures — P2

* **Scope:** Add reproducible CI for generated code, dependency integrity,
  race tests and bounded integration runs, plus backup/restore procedures.
* **Rationale:** Current checks rely on a local environment that could not be
  provisioned here, and there is no tested operator recovery runbook.
* **Dependencies:** Steps 1, 2, 5, 7 and 8.
* **Acceptance:** CI passes from a clean checkout without global installs;
  documented backup/restore drills preserve versions and deduplication state,
  and test artifacts and credentials are cleaned up.
* **Effort:** 10–20 hours.

## Total estimate

**302–558 engineering hours** under the assumptions above. This is not a claim
that existing unverified work is correct or that future test failures fit within
the range; safety review and external production-readiness work are additional.
