## Why

DataQL's sources have grown organically into three different shapes: remote
file handlers (`s3handler`, `gcshandler`, `azurehandler`, `urlhandler`,
`stdinhandler`) that download a URI to a local path; database connectors
(`dbconnector.Connector`); and message-queue readers (`mqreader`). The core
engine `internal/dataql/dataql.go` hard-codes each remote handler by hand, so
adding a source means editing the core, error handling is inconsistent, and two
sources (GCS, Azure) can't be pointed at an emulator — which is why their E2E
was deferred. A single, documented source contract makes the tool consistent,
testable, and safe to extend (the base for streaming and new sources).

This is intentionally **phased and low-risk**: the core engine has only ~14%
coverage, so we add the safety net (tests + emulator E2E) before refactoring it.

## What Changes

- **Phase 1 — Safety net (no behavior change):** add unit tests for the
  remote handlers and DB connectors that are near 0% (`gcshandler`,
  `azurehandler`, `s3handler`, `dbconnector`, `filehandler/database`), and a
  consistent error-wrapping helper they adopt.
- **Phase 2 — Emulator/endpoint support + E2E (the concrete unblock):** make
  GCS honor `STORAGE_EMULATOR_HOST` and Azure honor an emulator endpoint
  (azurite), then add GCS + Azure E2E (fake-gcs-server + azurite in
  docker-compose). S3 already works via `AWS_ENDPOINT_URL`.
- **Phase 3 — Unified `Source` contract + registry:** define a `Source`
  interface (`Scheme()`, `Matches(uri)`, `Fetch(uri) -> localPath, cleanup`)
  that the remote handlers implement, and a registry the core iterates instead
  of hard-coding each handler. Adding a source becomes "register a `Source`",
  not "edit the core". **No user-facing behavior change.**

## Capabilities

### New Capabilities
- `source-registry`: a documented source contract and registry so every remote
  source is resolved uniformly and new sources are added without editing the
  core engine.

### Modified Capabilities
- `e2e-harness`: GCS and Azure gain emulator-backed E2E coverage (previously
  blocked on endpoint support).

## Impact

- `internal/dataql/dataql.go` (source resolution → registry iteration).
- `pkg/gcshandler`, `pkg/azurehandler` (emulator endpoint support), `pkg/s3handler`.
- A new `pkg/source` package (the `Source` interface + registry + error helper).
- `e2e/docker-compose.yaml` (+ fake-gcs-server, azurite), new `e2e/tests/test-gcs.sh`, `test-azure.sh`.
- New unit tests across the touched packages; coverage rises.

## Non-goals

- Rewriting the DB `Connector` or `mqreader` interfaces (they stay; the registry
  wraps the remote-file sources first). A later change can fold DBs/MQ in.
- Streaming mode or new sources (Pulsar/RabbitMQ) — milestones 4 and 5.

## Self-contained invariant

All connectors remain compiled into the binary; the registry is internal wiring.
The new GCS/Azure emulator support is opt-in via env and does not add any runtime
dependency for normal use — the offline smoke test is unaffected.
