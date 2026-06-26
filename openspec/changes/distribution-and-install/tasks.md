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

## 4. arm64 release binaries

- [ ] 4.1 Add linux arm64 to `.goreleaser.yml` using the pinned Zig cross compiler (`CC="zig cc -target aarch64-linux-gnu"`); keep amd64 as-is.
- [ ] 4.2 Smoke-test each architecture's binary (offline) in CI before release.
- [ ] 4.3 Decide darwin/arm64 (native macOS runner vs Homebrew-from-source) and wire whichever is chosen.

## 5. Publishing (needs maintainer one-time setup)

- [ ] 5.1 Add goreleaser `dockers` + `docker_manifests` for multi-arch `ghcr.io/adrianolaselva/dataql`; `release.yml` logs into GHCR and pushes on tags.
- [ ] 5.2 Add goreleaser `brews` block publishing a formula to `adrianolaselva/homebrew-tap`.
- [ ] 5.3 Document the maintainer one-time setup (enable GHCR package, create `homebrew-tap` repo, add `HOMEBREW_TAP_TOKEN`); gate publish steps so PRs/forks don't fail without secrets.

## 6. Agent usage docs

- [ ] 6.1 Short per-agent setup guides (Claude Code, Codex, opencode) showing `dataql setup` and a sample MCP interaction.
- [ ] 6.2 Update `docs/mcp-setup.md` and `docs/llm-integration.md` to point at `dataql setup` instead of manual config editing.

## 7. Wrap-up

- [ ] 7.1 Keep the coverage ratchet green (add tests for new code; `make coverage-bump` if it rises).
- [ ] 7.2 Run `openspec validate distribution-and-install --strict`; full gate green.
- [ ] 7.3 Update the roadmap and archive the change once merged.
