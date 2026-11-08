# raftkv: project boundaries and working agreement

- Work only within this repository. Treat it as an independent project.
- Do not read, modify, clean up, or run commands in sibling repositories or the user's configuration.
- Work autonomously: choose sensible defaults and document assumptions; do not ask questions or delegate.
- Never install anything globally or use sudo, Homebrew, launchd, host credentials, or host Docker sockets.
- Dependencies, toolchains, caches, browser binaries, SDKs, temporary files and test data stay in this repository. Preserve the supplied project-local environment. Python packages go in `.venv`.
- `.agent-local/` is ignored, project-local build/runtime state. Do not bypass the sandbox.
- Use existing system tools read-only, or install pinned tools under this repository. Do not change shell profiles, user settings, Keychain, or global Git settings.
- Bind test servers to loopback; use ports 21800–21899. Limit build/test workers to two unless a required benchmark explicitly needs more.
- Docker-dependent integration tests must remain reproducible but be reported as unverified here if they require access outside this repository. Do not bypass this boundary.
- Do not touch attached hardware, enroll services, flash firmware, install agents, or alter OS settings. Supply manual checklists for such steps.
- Implement only the requested task, preserving prior behavior. Run appropriate checks, fix failures, and report genuine limitations without fabricating test or benchmark results.
- Before finishing each task: stop its servers/watchers, remove scratch outputs, update relevant documentation, and make a local conventional-commit Git commit. Do not push.
- End each task with a short summary of changes, checks actually run, remaining limitations, and commit hash.

## Go architecture and verification

- `internal/raft` owns consensus; implement Raft from scratch without hashicorp/raft or etcd/raft. `internal/kv` owns the state machine, `internal/transport` communication, and `cmd/raftkv-server` and `cmd/raftctl` the server and client entry points.
- Follow `DESIGN.md` as the protocol specification. Never hold a Raft lock during I/O or while sending on a channel that might block.
- Run `scripts/check.sh`: it first runs `go tool buf generate`, then `go build ./...`, `go test -race ./...`, `go vet ./...`, and `go tool golangci-lint run --concurrency 2`. Always run Go tests with `-race`; keep build/test parallelism at most two.
- Go and developer tools are pinned in `go.mod`; use `go tool` for the pinned developer tools, with dependencies and caches inside the repository. Install a matching Go toolchain locally if needed.
- The process chaos harness is `go run ./cmd/chaos -duration=10m -seed=20261107`; it owns loopback ports 21800–21844, starts five child servers, and deletes its temporary data after shutdown. It requires generated bindings and a working local Go toolchain.
- The bounded benchmark matrix is `go run ./cmd/bench -duration=5s -max-ops=20000`; it owns loopback ports 21800–21814 and removes temporary datasets. Its required 16/128-client cases are the explicit benchmark concurrency exception. Never use `-benchmark-no-fsync` with real data.
- Node telemetry uses `internal/observability` and `internal/traceutil`; metrics and health use the `-http-listen` address. Keep test listeners on loopback in the approved port range and shut down trace exporters and HTTP servers.
- Generated protobuf bindings and `go.sum` are tracked. Use `go tool buf generate` and `go mod tidy` after changing the schema or dependencies, and verify the generated files and module graph remain reproducible.
- Definition of done for each future task: `scripts/check.sh` passes; new code has tests; update `DESIGN.md` if the design changed; give a short summary; make a local conventional-commit Git commit. Report any genuine verification limitations rather than claiming a pass.
