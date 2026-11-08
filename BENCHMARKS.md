# Benchmarks

**No throughput or latency measurements are available from this environment.**
The Go toolchain is absent, the module proxy does not resolve, and generated
protobuf bindings and dependency checksums are missing. The benchmark command
was attempted but could not start. Numbers below are deliberately not inferred
or estimated.

## Matrix and results

Each cell requires Put, logged Get, and ReadIndex `ReadLatest` throughput and
p50/p99 latency. `—` means **not run**, not zero.

| Nodes | Concurrent clients | fsync | Put ops/s; p50/p99 | Get ops/s; p50/p99 | ReadIndex ops/s; p50/p99 |
| ---: | ---: | :---: | :--- | :--- | :--- |
| 3 | 1 | on | — | — | — |
| 3 | 16 | on | — | — | — |
| 3 | 128 | on | — | — | — |
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
At higher concurrency, batching may reduce replication RPC overhead, while
fsync and state-machine work can still dominate. Fsync-off is not a durability
equivalent and its effect depends on the filesystem and OS cache. Logged Get
participates in deduplication and returns the original result on retries;
ReadIndex avoids a read log entry but retries are independent reads. Their
contracts differ, so throughput differences are not a free, equivalent speedup.
No conclusion about the direction or magnitude of any effect is claimed until
the matrix is actually run.
