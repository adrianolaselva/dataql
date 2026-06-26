## Why

DataQL is now a matured, self-contained, gated binary (milestone 1), but it is
still tedious to adopt: the install script exists yet doesn't set up the agent
integration, the Dockerfile is broken (it builds a `csvql` binary on Go 1.18
with no entrypoint), there is no published image, no Homebrew tap, and no arm64
release. To hand DataQL to the community we need "install it and it just works"
across every surface — CLI, Docker, and agents — honoring the self-contained
invariant.

## What Changes

- **One-command install**: harden `scripts/install.sh` (and `install.ps1`) and
  document `go install`; the installed binary is the self-contained,
  DuckDB-embedded build and works offline immediately.
- **Agent auto-configuration**: a single command (`dataql setup`) registers the
  MCP server and installs the skills for **Claude Code, Codex, and opencode**,
  idempotently. The install script runs it so agents work with zero manual
  steps. **BREAKING**: none (additive command).
- **Complete Docker image**: rewrite the stale Dockerfile into a self-contained,
  glibc + DuckDB-embedded image with `dataql` as the entrypoint; publish it to
  **GHCR** (`ghcr.io/adrianolaselva/dataql`) from the release pipeline so
  `docker run --rm ghcr.io/adrianolaselva/dataql ...` works for one-off tasks.
- **Homebrew tap + arm64**: add linux/darwin **arm64** binaries to the release
  and a `brews:` block publishing a formula to a `homebrew-tap` repo.
- **"Stupidly simple" quickstart + agent guides**: a top-of-README quickstart
  and short per-agent setup docs (Claude Code, Codex, opencode).

## Capabilities

### New Capabilities
- `simple-install`: install DataQL via one command (script, Homebrew, or
  `go install`); the result is the self-contained binary, on linux/macOS,
  amd64/arm64.
- `agent-integration`: a command that auto-configures the MCP server and skills
  for the supported agents, idempotently, and which the installer invokes.
- `docker-image`: a complete, self-contained DataQL container image published to
  a public registry, runnable with no configuration.

### Modified Capabilities
- `self-contained-runtime`: extend the offline/self-contained guarantee to the
  Docker image and the arm64 binaries (the image and arm64 builds must embed
  DuckDB and pass the offline smoke).

## Impact

- **Build/release**: rewrite `Dockerfile`; `.goreleaser.yml` (arm64 builds,
  `dockers`/`brews` blocks); `release.yml` (GHCR login + push, arm64 toolchain).
- **CLI**: new `dataql setup` command (and library) under `cmd/` that writes
  agent MCP config + installs skills; extend `cmd/skillsctl` beyond Claude.
- **Scripts**: `scripts/install.sh`, `scripts/install.ps1` call `dataql setup`.
- **Docs**: README quickstart; `docs/` agent setup guides; `docs/mcp-setup.md`.
- **External (needs the maintainer's account)**: enabling GHCR package
  publishing and creating the `homebrew-tap` repository.

## Non-goals

- New data sources, streaming, or connector refactors (milestones 3–5).
- Signing/notarizing macOS binaries or Windows installers (future hardening).
- Auto-updating the binary in place.

## Self-contained invariant

This change strengthens the invariant: the Docker image and arm64 binaries are
added to the offline self-sufficiency checks, and `dataql setup` itself performs
no network calls (it only writes local agent config and unpacks embedded skills),
so the "install it and it works offline" promise holds on every new surface.
