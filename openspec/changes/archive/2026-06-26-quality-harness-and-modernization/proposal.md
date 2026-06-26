## Why

DataQL has grown into a capable tool but lacks a quality floor that guarantees
it never regresses: coverage sits at ~70% with no gate, several static-analysis
and vulnerability checks are disabled, E2E coverage is uneven, and dependencies
(Go, DuckDB, AWS SDK, …) have drifted behind current releases. Before adding
features and distributing widely, we need a foundation that makes quality
ratchet upward and makes the "install it and it just works, offline" promise
verifiable. This is milestone 1 of the roadmap — every later change depends on
it.

## What Changes

- Add a **coverage ratchet** to CI: a recorded baseline that the build refuses
  to drop below, with the target ramping toward >= 90%.
- **Harden static analysis**: re-enable high-value linters (govet, staticcheck,
  errcheck, gosec, ineffassign, …) in a phased, non-blocking-then-blocking way,
  and add **govulncheck** as a gate.
- Add **automated dependency freshness** (renovate or dependabot) so the tree
  stays current after this change.
- Standardize the **E2E harness**: one docker-compose stack, one `make e2e`
  entrypoint, a CI job, and coverage of every currently-shipped source/feature.
- Add an **offline self-sufficiency smoke test** that proves the binary runs
  with no network and no external services, and **embed DuckDB extensions**
  (httpfs, parquet, json, …) into the build so the first run never downloads.
- **Aggressive modernization**: bump to the latest stable Go and upgrade DuckDB,
  the AWS SDK, and other libraries to their current majors. Each risky upgrade
  is an isolated, independently verifiable task. **BREAKING** changes (if any
  surface in CLI flags or output) are documented and gated by the E2E suite.

## Capabilities

### New Capabilities
- `self-contained-runtime`: the binary embeds DuckDB + its extensions and all
  source drivers, runs fully offline, and downloads nothing at runtime.
- `quality-gates`: CI gates that prevent regression — coverage ratchet, static
  analysis, vulnerability scan, and dependency freshness.
- `e2e-harness`: a standardized, reproducible end-to-end test suite covering
  every shipped source and feature.

### Modified Capabilities
<!-- None: no existing openspec specs yet; modernization is delivered as tasks. -->

## Impact

- **Build/CI**: `.github/workflows/ci.yml`, new coverage-ratchet + govulncheck
  steps, new E2E job; `.golangci.yml` linter set; `Makefile` targets; new
  renovate/dependabot config.
- **Runtime/build**: DuckDB extension embedding in the build; possible CGO/build
  flag changes; `go.mod`/`go.sum` major upgrades (Go toolchain, DuckDB, AWS SDK,
  …).
- **Tests**: new offline smoke test; expanded unit tests to raise the ratchet;
  consolidated `e2e/` layout.
- **Docs**: contributor docs for running the harness; modernization notes for
  any behavior changes.

## Non-goals

- Adding new data sources or the streaming mode (milestones 4–5).
- The distribution/install UX and Docker publishing (milestone 2) — except the
  build-level embedding needed to make the offline guarantee true.
- Reaching exactly 90% coverage in this change. The gate is the *ratchet*; the
  number climbs over subsequent changes and must never fall.

## Self-contained invariant

This change *establishes and enforces* the self-contained invariant rather than
threatening it: extensions are embedded into the binary and the offline smoke
test becomes a permanent gate, so no later change can reintroduce a runtime
download or external dependency without failing CI.
