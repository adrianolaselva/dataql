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
| URL (http) | ✅ | local HTTP server | `tests/test-url.sh` (self-contained fixture server) |
| Local files | 🟡 | n/a | Per-format unit tests in `pkg/filehandler/*`; no dedicated E2E script yet |
| GCS | ❌ | — | **Blocked**: `gcshandler` has no custom-endpoint support, so a `fake-gcs-server` emulator can't be targeted. Needs connector endpoint override → roadmap milestone 3 (`connector-abstraction`) |
| Azure Blob | ❌ | — | **Blocked**: `azurehandler` hardcodes `*.blob.core.windows.net`, so `azurite` can't be targeted. Needs connector endpoint override → roadmap milestone 3 |
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

1. **GCS / Azure Blob** — blocked on connector endpoint-override support (the
   handlers can't target an emulator yet). Add `fake-gcs-server` / `azurite` to
   compose and `tests/test-gcs.sh` / `tests/test-azure.sh` once milestone 3
   (`connector-abstraction`) adds endpoint configuration.
2. **File formats** — add an E2E script asserting each format round-trips through
   `dataql run` (currently unit-only).
3. **MCP / describe / cache / export** — promote from unit to E2E assertions.

The E2E CI job is **advisory** until these gaps close and the suite is stable
in CI, after which it becomes a blocking gate (task 5.3).
