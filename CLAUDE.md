# CLAUDE.md

See [AGENTS.md](AGENTS.md) for the full agent guide. Key points:

- **Self-contained invariant**: the binary embeds DuckDB + extensions and all
  source drivers, runs offline, downloads nothing at runtime. Every feature
  ships embedded. Never introduce a runtime download or external dependency for
  a core path.
- **Quality never regresses**: coverage is ratcheted toward >= 90%; static
  analysis + `govulncheck` gate every PR. Never disable a gate to pass CI.
- **Spec-driven**: plan work as OpenSpec changes first. Roadmap in
  `openspec/roadmap.md`; changes in `openspec/changes/`. Use `/opsx:propose`,
  `/opsx:apply`, `/opsx:archive`.
- **Commits**: Conventional Commits. **Tests**: table-driven with testify.
- The `noduckdb` build tag is dev/test only — never for releases.
