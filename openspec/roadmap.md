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
| 1 | `quality-harness-and-modernization` | **detailed — next to implement** | Test/quality harness (coverage ratchet, hardened static analysis, govulncheck, standardized E2E infra, offline self-sufficiency smoke test) **plus** aggressive modernization (latest Go, DuckDB, AWS SDK and other libs to current majors; renovate/dependabot). Foundation before features. |
| 2 | `distribution-and-install` | planned | One-line install (script + Homebrew tap + `go install`) that auto-configures MCP + skills; complete Docker image published to GHCR; "stupidly simple" quickstart; agent usage guides (Claude Code, Codex, opencode). |
| 3 | `connector-abstraction` | planned | Unify the existing source connectors behind one interface (consistent auth, pagination, errors, tests, docs). All connectors compiled into the binary. Base for adding sources without regression. |
| 4 | `streaming-mode` | planned | First-class streaming: continuous consumption from topics/queues to stdout or windowed SQL. |
| 5 | `source-pulsar`, `source-rabbitmq` | planned | New embedded sources that are still missing (Kafka, SQS, S3, Postgres, MySQL, DynamoDB, Mongo already exist). |
| 6 | `llm-obvious-docs` | planned | Restructure docs to be obvious for LLMs: structured reference, runnable examples, sharp MCP tool/skill descriptions. Each milestone above also updates docs incrementally. |

## Ordering rationale

Harness + modernization come first so every later change lands on a stable,
gated, modern base and quality can only improve. Distribution next so the
community can install the matured tool trivially. Then the connector abstraction
de-risks all subsequent source/stream work, which is why streaming and new
sources follow it.
