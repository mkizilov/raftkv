# Benchmarks

The full matrix is **incomplete**. On 2026-11-08, a macOS/arm64 run with Go
1.24.10 and the project-local SDK completed six cases, then failed with
`context deadline exceeded` in the 3-node, 128-client, fsync-on Put case.
These six measurements were taken before the later queued-proposal cancellation
fix and are not a complete or directly comparable result for the final code.
A focused rerun after that fix still timed out and observed repeated term changes
under the same high concurrency without injected faults. The precise cause has
not been established. No values are inferred for the remaining cases.

## Matrix and results

Each cell shows operations/second and p50/p99 latency in milliseconds; latency
values are histogram upper bounds. `—` means **not run**, not zero. `*` means
the 20,000-operation cap ended the case before the five-second target.

| Nodes | Concurrent clients | fsync | Put ops/s; p50/p99 | Get ops/s; p50/p99 | ReadIndex ops/s; p50/p99 |
| ---: | ---: | :---: | :--- | :--- | :--- |
| 3 | 1 | on | 80.00; 11.7/33.7 | 82.25; 11.9/23.0 | 4,417.89; 0.2/0.8* |
| 3 | 16 | on | 290.28; 60.9/97.0 | 230.69; 71.7/101.8 | 17,933.17; 0.8/2.5* |
| 3 | 128 | on | deadline exceeded | — | — |
| 3 | 1 | off | — | — | — |
| 3 | 16 | off | — | — | — |
| 3 | 128 | off | — | — | — |
| 5 | 1 | on | — | — | — |
| 5 | 16 | on | — | — | — |
| 5 | 128 | on | — | — | — |
| 5 | 1 | off | — | — | — |
| 5 | 16 | off | — | — | — |
| 5 | 128 | off | — | — | — |

## Reproduce

After installing the pinned Go toolchain **inside this repository**, resolving
the pinned modules and generating bindings as described in `README.md`, run:

```sh
go run ./cmd/bench -duration=5s -max-ops=20000
```

The runner builds a server, starts a fresh real-process cluster for each of the
36 cases, uses loopback ports 21800–21814, and prints one JSON record per case.
Each case warms up with a committed Put, then runs the stated number of
concurrent clients against one key. It uses a five-second per-operation
deadline, stops on any failed operation, includes in-flight draining in elapsed
time, and removes the temporary data and child processes afterward. `-max-ops`
bounds per-case memory and data growth; the hard limit is 20,000 operations and
30 seconds per case. `StoppedAtLimit` reports an early cap.
The fixed-size latency histogram reports **upper bounds** in 100 µs buckets;
the overflow bucket reports the largest observed latency. Record machine,
filesystem, OS, power mode, command, repeats, and the full JSON output when
publishing actual results. Benchmark clients are the explicit exception to the
two-worker default: the 16- and 128-client cases require that concurrency.

`-benchmark-no-fsync` is an explicit **unsafe benchmark-only** server option.
It skips file and directory sync for the same disk-backed storage path; it can
lose acknowledged data after a crash or power loss. Normal server operation
always uses fsync. Never use the unsafe mode for real data.

## Interpreting future results

AppendEntries batches queued suffix entries, up to 64 entries or approximately
1 MiB per RPC; it does not batch the local fsync for individual proposals.
The completed cases show ReadIndex substantially faster than logged Get in
this particular short run, with both ReadIndex cases stopping at the operation
cap. At higher concurrency, batching may reduce replication RPC overhead, while
fsync and state-machine work can still dominate. Fsync-off is not a durability
equivalent and its effect depends on the filesystem and OS cache. Logged Get
participates in deduplication and returns the original result on retries;
ReadIndex avoids a read log entry but retries are independent reads. Their
contracts differ, so throughput differences are not a free, equivalent speedup.
No conclusion about fsync cost, 128-client throughput, or general performance
is justified by this incomplete single run.
