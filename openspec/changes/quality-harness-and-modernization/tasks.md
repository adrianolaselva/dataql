## 1. Coverage ratchet

- [x] 1.1 Add a committed coverage baseline file recording current total coverage (`go test -coverprofile` → `go tool cover -func` total). Baseline measured at **23.7%** over all packages (the previously committed 70.7% was generated over a cherry-picked subset and is not representative).
- [x] 1.2 Add a `make coverage-check` target + script that fails when total coverage < baseline, and a `make coverage-bump` to raise it. (`scripts/coverage-ratchet.sh`, `.coverage-baseline`).
- [x] 1.3 Wire the coverage check into `ci.yml` as a blocking step; document the bump workflow in CONTRIBUTING.md.

## 2. Static analysis hardening (phased)

- [x] 2.1 Add `govulncheck` as a new blocking CI job. Cleared all 15 affected-path vulns via security upgrades (Go toolchain 1.24→1.26.4, mapstructure v2.2.1→v2.5.0, x/net→v0.56.0); govulncheck now reports 0 affected vulnerabilities.
- [x] 2.2 Measured the backlog: errcheck 50, gosec 21, staticcheck 7 (govet/errorlint/misspell already clean). gosec fully triaged — see 2.3.
- [x] 2.3 Flipped **govet, staticcheck (SA*), errcheck, and gosec** to blocking (0 issues): fixed 7 staticcheck items + 7 unchecked `Scan()` silent failures; fixed both real G115 integer-overflow conversions (storage/type, dynamodb); justified G204 (constant `gh` executable) and G110 (user's own local files) with documented `#nosec`; excluded inherent-to-local-CLI rules (G104/G201/G304/G301/G306) via `scripts/gosec.sh` with recorded rationale; excluded opt-in QF*/style ST checks and non-actionable Close/print returns in `.golangci.yml`.

## 3. Self-contained runtime (embed DuckDB + extensions)

- [x] 3.1 Verified DuckDB format support (CSV, JSON, Parquet) is already built into the bundled go-duckdb engine — local/Parquet queries succeed offline (proven by the offline smoke). DataQL uses its own S3/GCS/Azure handlers, not DuckDB `httpfs`, so no extension needs separate embedding for the current feature set. No runtime download path exists for local formats.
- [~] 3.2 The default `go build` already produces a self-contained, DuckDB-included binary (this is what the offline smoke validates). Only the **musl/Alpine linux release** sets `noduckdb`, because go-duckdb's prebuilt static lib is glibc-based and won't link under musl. Switching the release to a glibc + Zig static build is a toolchain spike that needs the actual zig/goreleaser toolchain to validate (not available locally). **Pending CI validation — not landed to avoid breaking the working release.** Release Go image bumped 1.24→1.26.
- [ ] 3.3 Produce and verify a self-contained linux amd64/arm64 **release** binary with DuckDB embedded (depends on 3.2's toolchain spike; validate via `goreleaser build --snapshot` + offline smoke in CI).

## 4. Offline self-sufficiency smoke test

- [x] 4.1 Add `scripts/smoke-offline.sh` running core commands (version, CSV + JSON query via embedded DuckDB, export, `mcp serve`) with no external services. Passes locally. (Extension-dependent ops like httpfs/remote-parquet are exercised once task 3.1 embeds those extensions.)
- [x] 4.2 Add blocking `offline-smoke` CI job that builds the binary then runs the smoke under `sudo unshare -n` (network fully disabled), proving self-sufficiency.

## 5. E2E harness standardization

- [x] 5.1 Add a single root `make e2e` entrypoint (build → provision via docker-compose → run suite → tear down) with teardown-always and exit-status propagation (no silent `|| true` pass).
- [~] 5.2 Audited E2E coverage into `e2e/COVERAGE.md`. Covered today: Postgres, MySQL, Mongo, Kafka, SQS, S3, DynamoDB, install. **Remaining backfill (tracked):** URL, GCS (fake-gcs-server), Azure (azurite), file-format E2E, and promoting export/describe/cache/MCP from unit to E2E. RabbitMQ/Pulsar are roadmap milestone 5.
- [~] 5.3 Added an **advisory** `e2e` CI job (`make e2e`). Becomes blocking once the COVERAGE.md gaps close and the suite is stable; the gap list in COVERAGE.md serves as the coverage check until automated.

## 6. Aggressive modernization (one risky bump per task)

- [x] 6.1 Bump Go toolchain to the latest stable: `go.mod` go 1.26.0 / toolchain go1.26.4, workflows go-version 1.26; build + unit + E2E green. (Dockerfile base image handled in milestone 2's distribution work; it is currently stale.)
- [x] 6.2 DuckDB driver kept on `marcboeker/go-duckdb` v1.8.5 (latest of the v1 line). The v2 module (`/v2`) is a separate API migration and is deferred to its own change, tracked in the roadmap. Full gate green.
- [x] 6.3 Upgrade AWS SDK v2 modules to current (core v1.41.1→v1.42.0 plus config/credentials/s3/dynamodb/sqs); build + tests + govulncheck green.
- [x] 6.4 Upgrade remaining libraries to latest minors (cobra v1.10.2, excelize v2.10.1, mcp-go v0.43.2→v0.55.1, mongo-driver v1.17.9, kafka-go, go-sqlite3, lib/pq, mysql, goavro, color, progressbar). All majors were already current; no breaking-major upgrades were required. Full gate + lint + MCP smoke green.
- [x] 6.5 Add `.github/dependabot.yml` for gomod (grouped minor/patch), github-actions, and docker, each running the full CI gate.

## 7. Documentation & wrap-up

- [x] 7.1 Documented the quality gates (ratchet, lint, govulncheck, offline smoke, gosec/E2E advisory) in CONTRIBUTING.md; invariants in AGENTS.md/CLAUDE.md.
- [x] 7.2 No BREAKING CLI/output changes surfaced — all upgrades built and tested green with identical behavior (full test suite + lint + offline smoke).
- [ ] 7.3 Run `openspec validate --strict` and archive once the remaining items (3.2/3.3 release spike, 5.2/5.3 E2E backfill, gosec hardening) land. Validation passes now; archiving is deferred until those close.
