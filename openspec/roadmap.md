# DataQL Roadmap

Sequenced milestones for maturing DataQL into a self-contained, well-tested,
LLM-native data tool ready for the wider community. Each milestone becomes an
OpenSpec change, detailed just-in-time when it is picked up (the OpenSpec way —
avoid writing speculative specs that go stale).

## Cross-cutting principles (apply to every change)

- **Self-contained / batteries-included.** Installing DataQL installs its whole
  ecosystem. The binary embeds DuckDB + its extensions and every source
  driver/SDK; install auto-configures the MCP server and skills; the Docker
  image is a complete package. No runtime downloads, works offline. Every new
  feature ships embedded — verified by an offline smoke test.
- **Quality never regresses.** Coverage is ratcheted toward >= 90%; static
  analysis + vulnerability scanning gate every PR.
- **LLM-obvious docs.** Every feature ships docs and MCP/skill descriptions an
  LLM can use without guesswork.

## Milestones

| # | Change | Status | Summary |
|---|--------|--------|---------|
| 1 | `quality-harness-and-modernization` | **✅ done (PR #52, archived)** | Test/quality harness (coverage ratchet, hardened static analysis, govulncheck, standardized E2E infra, offline self-sufficiency smoke test) **plus** aggressive modernization (latest Go, AWS SDK and other libs; dependabot) and a self-contained release binary with DuckDB embedded. Bonus: fixed a real DB numeric-type import bug the harness caught. |
| 2 | `distribution-and-install` | **✅ done (PR #59, archived)** | Self-contained Docker image (GHCR on release) + `dataql setup` (agent MCP auto-config) + one-line install. *Follow-ups: arm64 binaries, Homebrew tap.* |
| 3 | `connector-abstraction` | **✅ done (PR #62, archived)** | `source.Resolver` registry (adding a remote source = register it); GCS/Azure emulator support + E2E (GCS live; Azure seed a follow-up); safety-net tests. Coverage 24%→41%. *Follow-ups: DB/MQ into the registry, `az://` alias, `source.WrapError` adoption.* |
| 4 | `streaming-mode` | **detailed — implementing** | First-class streaming: continuous consumption from topics/queues to stdout or windowed SQL. |
| 5 | `source-pulsar`, `source-rabbitmq` | planned | New embedded sources that are still missing (Kafka, SQS, S3, Postgres, MySQL, DynamoDB, Mongo already exist). |
| 6 | `llm-obvious-docs` | partly done | Restructure docs to be obvious for LLMs. README/Pages overhauled (PR #60); structured reference + sharper MCP tool descriptions remain. |

### Cross-cutting follow-ups (tracked, not milestones)
- Coverage push toward 90% (core `internal/dataql` 14%, `dbconnector`, `mcpctl`, `repl`, cloud handlers).
- M2: arm64 release binaries + Homebrew tap. · M3: Azure E2E seed hardening, `az://` alias, fold DB/MQ into the registry.

## Ordering rationale

Harness + modernization come first so every later change lands on a stable,
gated, modern base and quality can only improve. Distribution next so the
community can install the matured tool trivially. Then the connector abstraction
de-risks all subsequent source/stream work, which is why streaming and new
sources follow it.
