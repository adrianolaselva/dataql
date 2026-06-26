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

### Requirement: All distribution artifacts are self-contained
Every published distribution artifact SHALL embed the DuckDB engine and pass the
offline self-sufficiency check. This covers the Docker image and the per-arch
(amd64/arm64) release binaries. No published artifact MUST rely on a runtime
download or external service for core functionality.

#### Scenario: Docker image runs offline
- **WHEN** the published image runs a local-file query with networking disabled
- **THEN** it succeeds using the embedded engine

#### Scenario: arm64 binary is self-contained
- **WHEN** the arm64 release binary runs the offline smoke
- **THEN** it passes, proving DuckDB is embedded for that architecture

### Requirement: Agent setup performs no network access
The `dataql setup` command SHALL configure agents using only embedded assets and
local file writes, performing no network calls.

#### Scenario: Setup runs with the network disabled
- **WHEN** `dataql setup` runs on a host with networking disabled
- **THEN** it still installs skills and writes agent MCP config successfully

