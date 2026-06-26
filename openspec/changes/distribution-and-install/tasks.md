## 1. Docker image (self-contained, validated in CI)

- [ ] 1.1 Rewrite `Dockerfile`: minimal glibc runtime (`debian:bookworm-slim`), COPY the release `dataql` binary, `ENTRYPOINT ["dataql"]`, `WORKDIR /data`. Remove the stale `csvql`/Go-1.18 build.
- [ ] 1.2 Add a CI job that builds the image (no push) and runs an offline query through it (`docker run … run -f … -q …`), gating regressions.
- [ ] 1.3 Update `make docker-build` to build the new image and document `docker run` usage.

## 2. `dataql setup` command (agent auto-config)

- [ ] 2.1 Add a `setup` cobra command under `cmd/` with `--agent`, `--global/--project`, `--dry-run` flags.
- [ ] 2.2 Implement agent config writers (merge + idempotent, backup on change, no network): Claude Code (`mcpServers.dataql`), Codex (`~/.codex/config.toml` `mcp_servers.dataql`), opencode (config JSON `mcp.dataql`).
- [ ] 2.3 Install skills for each detected agent (extend `cmd/skillsctl` beyond Claude). Report what was configured; succeed when none present.
- [ ] 2.4 Unit tests against fixture configs: fresh write, idempotent re-run, preserve unrelated entries, no-agent case, dry-run.

## 3. Install scripts & quickstart

- [ ] 3.1 Harden `scripts/install.sh`: verify the downloaded binary, run `dataql setup` at the end (opt-out flag), print the next step.
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
