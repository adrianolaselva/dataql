# E2E Coverage Matrix

Tracks end-to-end coverage for every shipped source and major feature, per the
`e2e-harness` capability of the `quality-harness-and-modernization` change.
Run the full suite with `make e2e` from the repo root.

Legend: ✅ covered by an E2E script · 🟡 covered only by unit tests · ❌ gap

## Data sources

| Source | E2E | Backend in `make e2e` | Notes |
|--------|-----|-----------------------|-------|
| PostgreSQL | ✅ | `postgres` container | `tests/test-postgres.sh` |
| MySQL | ✅ | `mysql` container | `tests/test-mysql.sh` |
| MongoDB | ✅ | `mongodb` container | `tests/test-mongodb.sh` |
| Kafka | ✅ | `kafka` container | `tests/test-kafka.sh` |
| SQS | ✅ | LocalStack | `tests/test-sqs.sh` |
| S3 | ✅ | LocalStack | `tests/test-s3.sh` |
| DynamoDB | ✅ | LocalStack | `tests/test-dynamodb.sh` |
| Local files | 🟡 | n/a | Per-format unit tests in `pkg/filehandler/*`; no dedicated E2E script yet |
| URL (http) | ❌ | — | No E2E; needs a fixture HTTP server |
| GCS | ❌ | — | No emulator in compose (`fake-gcs-server` candidate) |
| Azure Blob | ❌ | — | No emulator in compose (`azurite` candidate) |
| RabbitMQ | ❌ | — | Source not yet implemented (roadmap milestone 5) |
| Apache Pulsar | ❌ | — | Source not yet implemented (roadmap milestone 5) |

## Features

| Feature | E2E | Notes |
|---------|-----|-------|
| Install / binary | ✅ | `tests/test-install.sh` |
| Export formats | 🟡 | Unit-tested in `pkg/exportdata/*`; E2E assertion pending |
| describe | 🟡 | Unit-tested in `internal/dataql` + `cmd/describectl` |
| cache | 🟡 | Unit-tested in `pkg/cachehandler` |
| REPL | 🟡 | Unit-tested in `pkg/repl` |
| MCP server | 🟡 | Smoke-checked in the offline self-sufficiency job |

## Remaining backfill (tracked)

To reach "E2E for every shipped source", the following are the known gaps:

1. **GCS** — add `fakegcs` (fsouza/fake-gcs-server) to `docker-compose.yaml` and a
   `tests/test-gcs.sh`.
2. **Azure Blob** — add `azurite` to compose and a `tests/test-azure.sh`.
3. **URL** — add a static HTTP fixture server and a `tests/test-url.sh`.
4. **File formats** — add an E2E script asserting each format round-trips through
   `dataql run` (currently unit-only).
5. **MCP / describe / cache / export** — promote from unit to E2E assertions.

The E2E CI job is **advisory** until these gaps close and the suite is stable
in CI, after which it becomes a blocking gate (task 5.3).
