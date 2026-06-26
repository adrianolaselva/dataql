## ADDED Requirements

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
