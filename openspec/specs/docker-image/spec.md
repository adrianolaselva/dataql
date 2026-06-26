# docker-image Specification

## Purpose
TBD - created by archiving change distribution-and-install. Update Purpose after archive.
## Requirements
### Requirement: Complete, self-contained Docker image
DataQL SHALL publish a container image to a public registry
(`ghcr.io/adrianolaselva/dataql`) that contains the self-contained,
DuckDB-embedded binary as its entrypoint and requires no configuration to run.

#### Scenario: Run a one-off query via Docker
- **WHEN** a user runs `docker run --rm -v "$PWD":/data ghcr.io/adrianolaselva/dataql run -f /data/file.csv -q "SELECT 1"`
- **THEN** the query executes and returns results with no additional setup

#### Scenario: Image entrypoint is dataql
- **WHEN** the image is run with arguments
- **THEN** the arguments are passed to the `dataql` CLI (the entrypoint), e.g.
  `docker run --rm ghcr.io/adrianolaselva/dataql --version` prints the version

### Requirement: Image is built and validated in CI
CI SHALL build the Docker image on pull requests (without pushing) and verify it
runs a query offline, so the image cannot regress.

#### Scenario: PR builds and smoke-tests the image
- **WHEN** a pull request changes the Dockerfile or release build
- **THEN** CI builds the image and runs an offline query through it successfully

