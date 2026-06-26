# self-contained-runtime Specification

## Purpose
TBD - created by archiving change quality-harness-and-modernization. Update Purpose after archive.
## Requirements
### Requirement: Single self-contained binary
The released DataQL binary SHALL embed the DuckDB engine and all source drivers
compiled into it. The binary MUST NOT require the user to install DuckDB, a
database client, a language runtime, or any other component for core
functionality to work.

#### Scenario: Query a local file on a clean machine
- **WHEN** a user installs only the DataQL binary on a machine with no DuckDB,
  no Go toolchain, and no other DataQL dependency installed
- **THEN** `dataql run -f data.csv -q "SELECT * FROM data LIMIT 1"` returns
  results without any additional installation step

#### Scenario: Release artifact includes the engine
- **WHEN** a release binary is produced by the release pipeline
- **THEN** it is built with the DuckDB engine included (the `noduckdb` stub
  build is never published as a release artifact)

### Requirement: Runs fully offline with no runtime downloads
DataQL SHALL perform all core operations without network access and without
downloading any component (including DuckDB extensions) at runtime. Required
DuckDB extensions SHALL be embedded so the first invocation needs no network.

#### Scenario: First run with network disabled
- **WHEN** DataQL is run for the first time on a host with networking disabled
- **THEN** querying a local file and any operation relying on a bundled
  extension succeeds without attempting a download

#### Scenario: No external services required
- **WHEN** DataQL executes a core command (file query, describe, export, REPL,
  MCP serve smoke)
- **THEN** it completes without requiring any external service to be running

### Requirement: New capabilities ship embedded
Every new feature or data source added to DataQL SHALL be compiled into the
binary and preserve the offline, self-contained guarantee. No feature may
introduce a mandatory runtime download or external dependency for its core path.

#### Scenario: A newly added source preserves self-containment
- **WHEN** a new source or feature is added in a later change
- **THEN** the offline self-sufficiency smoke test still passes for the build
  that includes it

