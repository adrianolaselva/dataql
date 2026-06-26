## 1. Docker image (self-contained, validated in CI)

- [x] 1.1 Rewrote `Dockerfile`: multi-stage glibc build (golang:1.26-bookworm → debian:bookworm-slim) with DuckDB embedded (release ldflags), `ENTRYPOINT ["dataql"]`, `WORKDIR /data`, ca-certificates. Added `.dockerignore`. **Validated locally**: 252MB image; `--version`, a mounted-CSV query, and a `--network none` GROUP BY query all work.
- [x] 1.2 Added a `docker` CI job that builds the image (no push) and runs an **offline** (`--network none`) query through it.
- [x] 1.3 Updated `make docker-build` (+ `make docker-smoke`) and added a "Run with Docker" section to the README.

## 2. `dataql setup` command (agent auto-config)

- [x] 2.1 Added `dataql setup` (`cmd/setupctl`) with `--agent` and `--dry-run` flags; registered in `cmd/main.go`.
- [x] 2.2 Implemented agent config writers — merge + idempotent + backup (`.bak`) + no network: Claude Code (`~/.claude.json` `mcpServers.dataql`), Codex (`~/.codex/config.toml` `[mcp_servers.dataql]` appended without a TOML dep), opencode (`~/.config/opencode/opencode.json` `mcp.dataql`). **Validated end-to-end** against a temp HOME (configures 3 agents, preserves existing keys, idempotent re-run).
- [~] 2.3 `setup` configures the MCP server for each detected agent and points to `dataql skills install` for skills. Per-agent skill auto-install (beyond Claude) is a follow-up — `skillsctl`'s installer needs exporting; tracked.
- [x] 2.4 Unit tests cover fresh write, idempotent re-run, preserve-unrelated, invalid JSON, dry-run (no write), Codex append idempotency, opencode shape. Coverage rose 23.7%→24.1% (baseline bumped).

## 3. Install scripts & quickstart

- [x] 3.1 `scripts/install.sh` now runs `dataql setup` after install (with a `--no-setup` opt-out) and prints next steps; syntax-checked.
- [ ] 3.2 Mirror the changes in `scripts/install.ps1` (Windows) where applicable.
- [ ] 3.3 Add a top-of-README "stupidly simple" quickstart (install → first query in <1 min) and document `go install`.

## 4. arm64 release binaries (follow-up — needs CI validation)

- [ ] 4.1 Add linux arm64 to `.goreleaser.yml` via the Zig cross compiler (`CC="zig cc -target aarch64-linux-gnu"`). **Deferred**: needs zig wired into `release.yml` and a real release run to validate (can't be checked locally); amd64 ships now.
- [ ] 4.2 Smoke-test each architecture's binary (offline) in CI before release.
- [ ] 4.3 Decide darwin/arm64 (native macOS runner vs Homebrew-from-source).

## 5. Publishing

- [x] 5.1 GHCR publish wired: `release.yml` logs into GHCR with `GITHUB_TOKEN` (added `packages: write`) and builds+pushes the validated self-contained image as `ghcr.io/adrianolaselva/dataql:<tag>` and `:latest` on tags (amd64). Multi-arch image is a follow-up (4.1).
- [ ] 5.2 Homebrew tap (`brews` in goreleaser → `adrianolaselva/homebrew-tap`). **Deferred**: needs the maintainer to create the `homebrew-tap` repo + a token; the script-install path covers macOS meanwhile.
- [~] 5.3 GHCR publish needs no extra secret (uses `GITHUB_TOKEN`); document the Homebrew one-time setup when 5.2 lands.

## 6. Agent usage docs

- [~] 6.1 `docs/mcp-setup.md` now leads with `dataql setup`; dedicated per-agent sample-interaction guides are a small follow-up.
- [x] 6.2 Updated `docs/mcp-setup.md` to lead with `dataql setup` over manual config editing.

## 7. Wrap-up

- [x] 7.1 Coverage ratchet green; rose to 24.1% with setupctl tests (baseline bumped).
- [ ] 7.2 Run `openspec validate distribution-and-install --strict`; full gate green (local gates green; CI runs on PR).
- [ ] 7.3 Update the roadmap and archive the change once merged.
