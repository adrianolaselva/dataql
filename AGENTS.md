# Agent guide for DataQL

DataQL is a Go CLI that queries and manipulates data from files and remote
sources using SQL, powered by an embedded DuckDB engine. It is LLM-native: it
ships an MCP server and installable skills so agents get small query results
instead of raw data.

## Project invariants — never violate these

1. **Self-contained / batteries-included.** Installing DataQL installs its whole
   ecosystem. The binary embeds DuckDB + its extensions and every source driver;
   it runs fully offline with no runtime downloads and no external services.
   Every new feature ships embedded. An offline smoke test enforces this — if a
   change would require a runtime download or external dependency for a core
   path, it is wrong.
2. **Quality never regresses.** Test coverage is ratcheted (CI fails if total
   coverage drops below the committed baseline) toward >= 90%. Static analysis
   and `govulncheck` gate every PR. Do not disable a gate to make CI pass.
3. **LLM-obvious.** Every user-facing feature ships docs and MCP/skill
   descriptions clear enough for an LLM to use without guesswork.

## Spec-driven workflow (OpenSpec)

This project plans work as OpenSpec changes before implementing.

- Roadmap and milestone order: `openspec/roadmap.md`.
- Project context for AI: `openspec/config.yaml`.
- Active/planned changes: `openspec/changes/<name>/` (proposal, design, specs,
  tasks).
- Before building a feature, there should be an approved change. Use
  `/opsx:propose` to create one, `/opsx:apply` to implement its tasks, and
  `/opsx:archive` when done. Validate with
  `openspec validate <change> --strict`.

## Conventions

- Conventional Commits: `feat(scope):`, `fix(scope):`, `chore(scope):`, etc.
- Table-driven tests with `testify`. Small, single-purpose packages.
- Match existing patterns in the package you touch; keep changes scoped.

## Layout

- `main.go` → `cmd/` (cobra: run, describe, cache, mcp, skills)
- `pkg/` connectors & handlers; `internal/dataql`, `internal/exportdata` core
- `e2e/` docker-compose-driven end-to-end tests
- `.github/workflows/` CI; `.golangci.yml` lint; `.goreleaser.yml` release

## Key commands

- Build: `make build` · Test: `make test` · Coverage: `make coverage`
- Lint/format: `make check` (`fmt-check` + `lint`)
- E2E: see `e2e/` (standardized under `make e2e` by the quality-harness change)
- The `noduckdb` build tag is for dev/test only — never for releases.
