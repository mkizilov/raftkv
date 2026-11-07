# raftkv: project boundaries and working agreement

- Work only in `/Users/mk-rune/test_projects/09-raftkv`. This is one of fourteen independent repositories.
- Do not read, modify, clean up, or run commands in sibling repositories or the user's configuration.
- Work autonomously: choose sensible defaults and document assumptions; do not ask questions or delegate.
- Never install anything globally or use sudo, Homebrew, launchd, host credentials, or host Docker sockets.
- Dependencies, toolchains, caches, browser binaries, SDKs, temporary files and test data stay in this repository. Preserve the supplied project-local environment. Python packages go in `.venv`.
- `.agent-local/` is ignored, project-local build/runtime state. `.opencode/` is managed orchestration configuration; do not change or remove it or bypass the sandbox.
- Use existing system tools read-only, or install pinned tools under this repository. Do not change shell profiles, user settings, Keychain, or global Git settings.
- Bind test servers to loopback; use ports 21800–21899. Limit build/test workers to two unless a required benchmark explicitly needs more.
- Docker-dependent integration tests must remain reproducible but be reported as unverified here if they require access outside this repository. Do not bypass this boundary.
- Do not touch attached hardware, enroll services, flash firmware, install agents, or alter OS settings. Supply manual checklists for such steps.
- Implement only the current numbered prompt, preserving prior behavior. Run appropriate checks, fix failures, and report genuine limitations without fabricating test or benchmark results.
- Before finishing every numbered turn: stop its servers/watchers, remove scratch outputs, update relevant documentation, and make a local conventional-commit Git commit. Do not push.
- End each turn with a short summary of changes, checks actually run, remaining limitations, and commit hash. The orchestrator will provide the next prompt only after this turn ends.

Extend this file with the project's language-specific architecture and verification commands while retaining these boundaries.

## Go architecture and verification

- `internal/raft` owns consensus; implement Raft from scratch without hashicorp/raft or etcd/raft. `internal/kv` owns the state machine, `internal/transport` communication, and `cmd/raftkv-server` and `cmd/raftctl` the server and client entry points.
- Follow `DESIGN.md` as the protocol specification. Never hold a Raft lock during I/O or while sending on a channel that might block.
- Run `scripts/check.sh`: it runs `go build ./...`, `go test -race ./...`, `go vet ./...`, and `go tool golangci-lint run --concurrency 2`. Always run Go tests with `-race`; keep build/test parallelism at most two.
- Go and developer tools are pinned in `go.mod`; use `go tool` for the pinned developer tools, with dependencies and caches inside the repository. Install a matching Go toolchain locally if needed.
- Initial dependency resolution and `go.sum` generation require a network-accessible Go module proxy: run `go mod tidy` using the pinned toolchain before the first check if checksums are absent.
- Definition of done for each future task: `scripts/check.sh` passes; new code has tests; update `DESIGN.md` if the design changed; give a short summary; make a local conventional-commit Git commit. Report any genuine verification limitations rather than claiming a pass.
