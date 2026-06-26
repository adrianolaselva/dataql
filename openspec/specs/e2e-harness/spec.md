# e2e-harness Specification

## Purpose
TBD - created by archiving change quality-harness-and-modernization. Update Purpose after archive.
## Requirements
### Requirement: Single-entrypoint reproducible E2E suite
The project SHALL provide one command (e.g. `make e2e`) that provisions any
required backing services via docker-compose, runs the full end-to-end suite,
and tears them down. The suite MUST be reproducible on a clean checkout.

#### Scenario: Run the full suite locally
- **WHEN** a contributor runs the E2E entrypoint on a clean checkout with Docker
  available
- **THEN** backing services start, all E2E tests run, services are torn down,
  and the command reports a single pass/fail result

### Requirement: E2E coverage of every shipped source and feature
The E2E suite SHALL include at least one test for each currently shipped data
source and major feature (file formats, URL, S3, GCS, Azure, PostgreSQL, MySQL,
MongoDB, DynamoDB, SQS, Kafka, export formats, describe, cache, REPL, MCP). New
sources/features added later MUST add corresponding E2E coverage.

#### Scenario: A shipped source has E2E coverage
- **WHEN** the E2E suite runs
- **THEN** each shipped source and major feature is exercised by at least one
  end-to-end test against a real or emulated backend

#### Scenario: Adding a source without E2E coverage
- **WHEN** a change adds a new source but no E2E test for it
- **THEN** the harness's coverage check for sources flags the gap

### Requirement: E2E runs as a CI gate
CI SHALL run the E2E suite and MUST fail the build when an E2E test fails (after
an initial stabilization period in which the job may be advisory).

#### Scenario: An E2E test fails in CI
- **WHEN** an end-to-end test fails on a pull request and the E2E gate is active
- **THEN** the CI E2E job fails and the PR cannot be merged

### Requirement: End-to-end coverage of every shipped file format
The E2E suite SHALL exercise every shipped file format by running the real
`dataql` binary against fixture files and asserting the query results. The
covered formats MUST include CSV, JSON, JSONL, Parquet, Excel, XML, YAML, Avro,
and ORC.

#### Scenario: Each format is queried end-to-end
- **WHEN** the E2E format matrix runs
- **THEN** for each supported format, the binary loads a fixture and a SQL query
  returns the expected rows, proving the format works through the full pipeline

#### Scenario: A format regression is caught
- **WHEN** a change breaks reading of a supported format
- **THEN** the corresponding format-matrix E2E test fails

### Requirement: GCS and Azure Blob have emulator-backed E2E
The E2E suite SHALL cover Google Cloud Storage and Azure Blob Storage against
local emulators (e.g. fake-gcs-server and azurite), querying objects through the
real `dataql` binary. These were previously deferred because the handlers could
not target an emulator.

#### Scenario: Query a GCS object via the emulator
- **WHEN** the E2E suite runs with a GCS emulator and a seeded object
- **THEN** `dataql` reads `gs://<bucket>/<object>` from the emulator and a query
  returns the expected rows

#### Scenario: Query an Azure Blob via the emulator
- **WHEN** the E2E suite runs with azurite and a seeded blob
- **THEN** `dataql` reads the blob and a query returns the expected rows

