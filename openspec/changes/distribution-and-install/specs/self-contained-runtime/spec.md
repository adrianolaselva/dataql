## ADDED Requirements

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
