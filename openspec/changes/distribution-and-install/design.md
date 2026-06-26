## Context

Milestone 1 produced a self-contained, gated binary and a glibc release build
with DuckDB embedded. Distribution surfaces are uneven: `scripts/install.sh`
downloads release tarballs (linux/macOS, amd64/arm64) but doesn't set up agents;
`Dockerfile` is broken (Go 1.18, builds `csvql`, no entrypoint, no DuckDB);
there is no published image, Homebrew tap, or arm64 release artifact. Agents are
configured by hand (the user edits `~/.claude/settings.json`).

The MCP server runs as `dataql mcp serve` (stdio); skills install via
`dataql skills install` (currently Claude-only, to `~/.config/claude/skills/`).

## Goals / Non-Goals

**Goals:**
- "Install it and it just works" on CLI, Docker, and agents — offline,
  self-contained, idempotent.
- One command sets up all three supported agents (Claude Code, Codex, opencode).
- Published, complete Docker image; Homebrew formula; arm64 binaries.

**Non-Goals:**
- New sources/streaming/connector work (later milestones).
- Code signing/notarization; in-place self-update.

## Decisions

### D1. A first-class `dataql setup` command (not shell logic in install.sh)
Agent configuration lives in a Go command (`dataql setup [--agent ...]
[--global|--project]`), not in `install.sh`. Rationale: it is cross-platform
(macOS/Linux/Windows), unit-testable, idempotent, and usable standalone (a user
who `go install`s gets the same setup). `install.sh` simply calls
`dataql setup` at the end. Alternatives: duplicating config logic in
shell/PowerShell (rejected — untestable, drifts) or a separate installer
(rejected — violates self-contained).

`dataql setup` writes the MCP server entry and installs skills for each agent:
- **Claude Code**: `mcpServers.dataql = {command, args:["mcp","serve"]}` in the
  Claude config; skills to `~/.config/claude/skills/` (existing path).
- **Codex**: `mcp_servers.dataql` in `~/.codex/config.toml`.
- **opencode**: `mcp.dataql` in the opencode config JSON.
It MUST merge (never clobber an unrelated config), be idempotent (re-running is a
no-op), and make **no network calls** (skills are embedded; it only writes local
files). It detects which agents are present and reports what it did.

### D2. Docker image built from the release binary via goreleaser `dockers`
Rewrite `Dockerfile` to a minimal glibc runtime (`debian:bookworm-slim`) that
COPYs the goreleaser-built linux binary and sets `ENTRYPOINT ["dataql"]`. The
image is assembled and pushed by goreleaser's `dockers`/`docker_manifests` to
`ghcr.io/adrianolaselva/dataql` (multi-arch amd64+arm64). Rationale: the image
ships the exact validated release binary (DuckDB embedded), not a separately
compiled one. Alternative (build Go inside the Dockerfile) rejected — it would
re-introduce a second, unvalidated build path.

### D3. arm64 via the validated zig static toolchain
Reuse milestone 1's finding: build linux arm64 with `CC="zig cc -target
aarch64-linux-gnu"` so DuckDB links. darwin/arm64 builds natively on a macOS
runner (or is offered via Homebrew-from-source). Each arch's binary must pass the
offline smoke before release.

### D4. Homebrew tap via goreleaser `brews`
Add a `brews:` block publishing a formula to `adrianolaselva/homebrew-tap`
(a repo the maintainer creates). `brew install adrianolaselva/tap/dataql` then
installs the self-contained binary.

## Risks / Trade-offs

- **GHCR push + tap repo need the maintainer's account/permissions** → Land the
  config and workflow; gate the publish on secrets/repo existing; document the
  one-time setup. CI builds the image as a no-push artifact on PRs to validate.
- **arm64 cross-compile flakiness (zig)** → Pin zig; smoke-test each arch; keep
  amd64 as the guaranteed path.
- **Editing user agent configs is invasive** → Strict merge + idempotency + a
  `--dry-run` and backup of the touched file; never remove unrelated keys.
- **Agent config formats drift** → Keep each agent writer small and covered by
  unit tests against fixture configs.

## Migration Plan

1. Rewrite Dockerfile + add a non-pushing image build to CI (validates the image
   + offline smoke) — safe, no external deps.
2. Add `dataql setup` with unit tests; wire `install.sh`/`install.ps1` to call it.
3. Add arm64 builds; smoke each arch.
4. Add goreleaser `dockers`/`brews`; enable GHCR push + create the tap (maintainer
   one-time); a tagged release then publishes image + formula.
Rollback: each surface is independent; revert its commit. The existing
script-install path keeps working throughout.

## Open Questions

- darwin/arm64 in CI: use a macOS runner now, or ship darwin via Homebrew-from-
  source first? (Lean: linux arm64 now; darwin arm64 when a macOS runner is wired.)
- Exact Claude config file/path precedence (`~/.claude.json` vs settings.json) —
  confirm during implementation against current Claude Code.
