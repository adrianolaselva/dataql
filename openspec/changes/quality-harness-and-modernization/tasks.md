## 1. Coverage ratchet

- [x] 1.1 Add a committed coverage baseline file recording current total coverage (`go test -coverprofile` → `go tool cover -func` total). Baseline measured at **23.7%** over all packages (the previously committed 70.7% was generated over a cherry-picked subset and is not representative).
- [x] 1.2 Add a `make coverage-check` target + script that fails when total coverage < baseline, and a `make coverage-bump` to raise it. (`scripts/coverage-ratchet.sh`, `.coverage-baseline`).
- [x] 1.3 Wire the coverage check into `ci.yml` as a blocking step; document the bump workflow in CONTRIBUTING.md.

## 2. Static analysis hardening (phased)

- [x] 2.1 Add `govulncheck` as a new blocking CI job. Cleared all 15 affected-path vulns via security upgrades (Go toolchain 1.24→1.26.4, mapstructure v2.2.1→v2.5.0, x/net→v0.56.0); govulncheck now reports 0 affected vulnerabilities.
- [x] 2.2 Measured the backlog: errcheck 50, gosec 21, staticcheck 7 (govet/errorlint/misspell already clean). gosec runs as an advisory (non-blocking) CI job pending security triage.
- [x] 2.3 Flipped **govet, staticcheck (SA*), and errcheck** to blocking in `.golangci.yml` (0 issues): fixed 7 staticcheck items + 7 unchecked `Scan()` silent failures in describe stats; excluded opt-in QF*/style ST checks and non-actionable Close/print returns. **gosec remains advisory** — G110/G115 (real) + G304/G201/G204 (inherent to a file/SQL tool) to be hardened in a focused follow-up.

## 3. Self-contained runtime (embed DuckDB + extensions)

- [ ] 3.1 Identify the DuckDB extensions DataQL actually uses (start: httpfs, parquet, json) and embed/preload them so no runtime download occurs.
- [ ] 3.2 Replace the release build's `noduckdb` tag with a portable static CGO build using a pinned Zig cross compiler (`CC="zig cc -target ..."`) in `.goreleaser.yml`; keep `noduckdb` for dev/test only.
- [ ] 3.3 Build a release binary for linux amd64/arm64 and confirm DuckDB is embedded (engine-included build, not the stub).

## 4. Offline self-sufficiency smoke test

- [ ] 4.1 Add a smoke test that runs core commands (file query, bundled-extension op, `--version`, `mcp serve` smoke) with networking disabled and no external services.
- [ ] 4.2 Add a CI job that runs the smoke test against the engine-included build and gates the release.

## 5. E2E harness standardization

- [ ] 5.1 Consolidate `e2e/` to one `make e2e` entrypoint that brings up docker-compose, runs the suite, and tears down.
- [ ] 5.2 Audit current E2E coverage and add tests so every shipped source/feature (files, URL, S3, GCS, Azure, Postgres, MySQL, Mongo, DynamoDB, SQS, Kafka, exports, describe, cache, REPL, MCP) is exercised against a real/emulated backend.
- [ ] 5.3 Add an E2E CI job (advisory first, then blocking) and a check that flags shipped sources lacking E2E coverage.

## 6. Aggressive modernization (one risky bump per task)

- [x] 6.1 Bump Go toolchain to the latest stable: `go.mod` go 1.26.0 / toolchain go1.26.4, workflows go-version 1.26; build + unit + E2E green. (Dockerfile base image handled in milestone 2's distribution work; it is currently stale.)
- [x] 6.2 DuckDB driver kept on `marcboeker/go-duckdb` v1.8.5 (latest of the v1 line). The v2 module (`/v2`) is a separate API migration and is deferred to its own change, tracked in the roadmap. Full gate green.
- [x] 6.3 Upgrade AWS SDK v2 modules to current (core v1.41.1→v1.42.0 plus config/credentials/s3/dynamodb/sqs); build + tests + govulncheck green.
- [x] 6.4 Upgrade remaining libraries to latest minors (cobra v1.10.2, excelize v2.10.1, mcp-go v0.43.2→v0.55.1, mongo-driver v1.17.9, kafka-go, go-sqlite3, lib/pq, mysql, goavro, color, progressbar). All majors were already current; no breaking-major upgrades were required. Full gate + lint + MCP smoke green.
- [x] 6.5 Add `.github/dependabot.yml` for gomod (grouped minor/patch), github-actions, and docker, each running the full CI gate.

## 7. Documentation & wrap-up

- [ ] 7.1 Document the harness (coverage ratchet, linters, govulncheck, E2E, offline smoke) and the modernization in CONTRIBUTING.md / docs.
- [ ] 7.2 Note any BREAKING CLI/output changes surfaced by modernization.
- [ ] 7.3 Run `openspec validate quality-harness-and-modernization --strict` and confirm all gates are green before archiving.
