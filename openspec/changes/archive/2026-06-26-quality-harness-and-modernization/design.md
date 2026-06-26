## Context

DataQL is a CGO program: its query engine is DuckDB via `marcboeker/go-duckdb`.
Today the **released binaries are built with the `noduckdb` tag** (see
`.goreleaser.yml`, `pkg/storage/duckdb/storage_stub.go`,
`pkg/dbconnector/duckdb_stub.go`) to dodge GLIBC/static-linking problems —
"users who need DuckDB can build from source." That means the shipped artifact
is **not** self-contained: its core engine is absent. This change makes the
self-contained promise real and permanent while raising the quality floor.

Current state we build on:
- CI: `build`, `test` (race + coverage, no gate), `lint` (golangci-lint v2 with
  many high-value linters disabled). Honest total coverage is **~23.7%** over
  all packages — the previously committed `coverage.out` (70.7%) was generated
  over a cherry-picked subset and overstated reality.
- E2E: `e2e/` with docker-compose, Makefile, scripts — present but not wired as
  a gating CI job and unevenly covering sources.
- Go 1.24; DuckDB, AWS SDK and other deps trailing current releases.

## Goals / Non-Goals

**Goals:**
- Ship a **self-contained binary** that embeds DuckDB + extensions and all
  source drivers, runs offline, downloads nothing at runtime.
- Make quality **ratchet upward**: coverage cannot fall below a recorded
  baseline; static analysis + `govulncheck` gate every PR.
- A **reproducible E2E harness** with one entrypoint, gating in CI, covering
  every shipped source/feature.
- **Modernize** to latest stable Go and current major versions of key libs,
  each risky bump isolated and verifiable.

**Non-Goals:**
- New sources / streaming mode (later milestones).
- Distribution UX, Homebrew, GHCR publishing (milestone 2) beyond the build
  changes needed for the offline guarantee.
- Hitting 90% coverage now — only ratcheting toward it.

## Decisions

### D1. Portable self-contained binaries via a static CGO toolchain (drop `noduckdb` for releases)
The `noduckdb` path exists only to avoid GLIBC issues with `-extldflags
-static`. Instead of removing DuckDB, we make the static CGO build portable:
- Build release binaries with a **Zig-based cross C compiler**
  (`CC="zig cc -target <triple>"`), which goreleaser supports and which reliably
  produces portable static binaries across libc versions for linux amd64/arm64.
- Keep the `noduckdb` tag as a **dev/test convenience only**, never for release.
- Alternatives considered: (a) dynamic linking per-distro — rejected, breaks
  "single binary, just works"; (b) musl cross toolchains per target — workable
  but heavier to maintain than zig; (c) a pure-Go engine — no mature DuckDB
  equivalent, rejected.

### D2. Embed DuckDB extensions; verify offline
DuckDB normally autoloads/downloads extensions (httpfs, parquet, json, …) on
first use. We bundle the required extensions and load them locally so the first
run needs no network. Implementation is validated behaviorally by an **offline
smoke test** (D3) rather than by asserting on DuckDB internals, so the test
stays valid across DuckDB upgrades.

### D3. Offline self-sufficiency smoke test as a permanent gate
A test (and a CI job) runs representative commands — local file query, an
embedded-extension operation, `--version`, MCP serve smoke — with **network
disabled** and **no external services**, asserting success. This becomes the
guardrail that stops any future change from reintroducing a runtime download or
external dependency.

### D4. Coverage ratchet over a hard 90% gate
A committed baseline file records the current total coverage. CI computes
coverage and **fails if it drops below the baseline**; when it rises, the
baseline is bumped (script-assisted). This satisfies "never regress" without
blocking all work at 70%. Target ramps to >= 90% across milestones.

### D5. Phased static-analysis hardening
Re-enabling staticcheck/gosec/errcheck/etc. all at once would flood the diff.
We enable them in **report-only** mode first, fix findings in grouped tasks,
then flip each to **blocking**. `govulncheck` is added as a blocking gate from
the start (few/no findings expected).

### D6. Aggressive modernization, one risky bump per task
Go toolchain to latest stable; DuckDB driver, AWS SDK v2 modules, and other
libs to current majors. Each potentially-breaking upgrade is its **own task**
with its own verification (build + unit + E2E green) so a regression is easy to
bisect and revert. CLI/output behavior changes, if any, are caught by E2E and
documented as **BREAKING**.

## Risks / Trade-offs

- **Zig/static toolchain flakiness in CI** → Pin the zig version; cache it;
  keep a documented local repro; the offline smoke + E2E jobs catch breakage
  before release.
- **DuckDB/AWS major upgrades break behavior** → Isolated tasks + E2E gate +
  the ratchet; revert a single task if red.
- **Extension embedding bloats the binary / per-platform divergence** → Embed
  only the extensions DataQL actually uses; measure binary size; the offline
  smoke test is the source of truth for "works without network".
- **Ratchet baseline churn / merge conflicts on the baseline file** → Store a
  single number, update via a make target, document the workflow for
  contributors.
- **Re-enabled linters surface large backlogs** → Phased report-only → blocking
  rollout keeps each PR reviewable.

## Migration Plan

1. Land harness scaffolding non-blocking (ratchet in report mode, linters in
   report mode, E2E job added but allowed-to-fail) so main stays green.
2. Fix findings in grouped tasks; flip gates to blocking one by one.
3. Switch release builds to the zig static toolchain **with** DuckDB; verify the
   produced binary passes the offline smoke test on clean linux amd64/arm64.
4. Perform modernization bumps, one task each, E2E green after each.
Rollback: each gate and each upgrade is an isolated commit/task; revert
individually. The `noduckdb` dev path remains as a fallback build.

## Open Questions

- Exact set of DuckDB extensions to embed (start from what the code paths use:
  httpfs, parquet, json; confirm during implementation).
- Windows/macOS release matrix for the static-with-DuckDB build (linux first;
  decide darwin/windows targets during implementation).
- renovate vs dependabot (lean dependabot for zero-infra; revisit if grouping
  rules get complex).
