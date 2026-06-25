## 1. Coverage ratchet

- [ ] 1.1 Add a committed coverage baseline file recording current total coverage (`go test -coverprofile` → `go tool cover -func` total).
- [ ] 1.2 Add a `make coverage-check` target + script that fails when total coverage < baseline, and a `make coverage-bump` to raise it.
- [ ] 1.3 Wire the coverage check into `ci.yml` as a blocking step; document the bump workflow in CONTRIBUTING.md.

## 2. Static analysis hardening (phased)

- [ ] 2.1 Add `govulncheck` as a new blocking CI job (`go run golang.org/x/vuln/cmd/govulncheck ./...`).
- [ ] 2.2 Enable previously-disabled high-value linters (govet, staticcheck, errcheck, gosec, ineffassign, …) in report-only mode and capture the backlog.
- [ ] 2.3 Fix findings in grouped commits per linter; flip each linter to blocking once clean. Record the blocking set in `.golangci.yml`.

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

- [ ] 6.1 Bump Go toolchain to the latest stable in `go.mod`, workflows, and Dockerfile; build + unit + E2E green.
- [ ] 6.2 Upgrade the DuckDB driver (`marcboeker/go-duckdb`) to its current major; fix breakage; full gate green.
- [ ] 6.3 Upgrade AWS SDK v2 modules to current; fix breakage; full gate green.
- [ ] 6.4 Upgrade remaining libraries (cobra, mongo-driver, kafka-go, parquet/arrow, excelize, etc.) to current majors, grouping safe minors and isolating each risky major; full gate green per group.
- [ ] 6.5 Add automated dependency updates (dependabot config) targeting Go modules + the toolchain, running the full CI gate.

## 7. Documentation & wrap-up

- [ ] 7.1 Document the harness (coverage ratchet, linters, govulncheck, E2E, offline smoke) and the modernization in CONTRIBUTING.md / docs.
- [ ] 7.2 Note any BREAKING CLI/output changes surfaced by modernization.
- [ ] 7.3 Run `openspec validate quality-harness-and-modernization --strict` and confirm all gates are green before archiving.
