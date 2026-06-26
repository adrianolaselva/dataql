# simple-install Specification

## Purpose
TBD - created by archiving change distribution-and-install. Update Purpose after archive.
## Requirements
### Requirement: One-command install
DataQL SHALL be installable with a single command on Linux and macOS (amd64 and
arm64) via the install script, Homebrew, or `go install`. The installed artifact
MUST be the self-contained, DuckDB-embedded binary.

#### Scenario: Script install on a clean machine
- **WHEN** a user runs the documented `curl … | bash` install command on a clean
  Linux or macOS host
- **THEN** the `dataql` binary is installed on PATH and `dataql --version`
  succeeds without any further setup

#### Scenario: Homebrew install
- **WHEN** a user runs `brew install adrianolaselva/tap/dataql`
- **THEN** the self-contained `dataql` binary is installed and runnable

#### Scenario: go install
- **WHEN** a user runs the documented `go install` command with a Go toolchain
- **THEN** `dataql` builds and installs with the DuckDB engine embedded

### Requirement: Quickstart is obvious
The README SHALL open with a copy-pasteable quickstart that takes a new user from
install to a first successful query in under a minute, and the install script
SHALL print the next step on success.

#### Scenario: New user follows the quickstart
- **WHEN** a user follows the README quickstart top-to-bottom
- **THEN** they install DataQL and run a working query without reading further

