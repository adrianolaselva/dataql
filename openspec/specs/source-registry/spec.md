# source-registry Specification

## Purpose
TBD - created by archiving change connector-abstraction. Update Purpose after archive.
## Requirements
### Requirement: Unified source contract
DataQL SHALL define a single `Source` contract that every remote file source
(S3, GCS, Azure Blob, HTTP/URL, stdin) implements, exposing how it claims a URI
and fetches it to a local path. The core engine MUST resolve a URI through a
registry of sources rather than hard-coding each handler.

#### Scenario: A registered source resolves its URIs
- **WHEN** the engine is given a URI whose scheme a registered source claims
- **THEN** that source fetches the data and the engine processes it, without the
  core referencing the concrete handler type

#### Scenario: Adding a source does not edit the core
- **WHEN** a new remote source is added by registering a `Source`
- **THEN** no change to the core engine's dispatch code is required for it to work

### Requirement: Consistent source errors
Source errors SHALL be wrapped consistently, identifying the scheme, the
operation, and the URI, so failures are uniform and actionable across sources.

#### Scenario: A fetch failure is reported uniformly
- **WHEN** any source fails to fetch a URI
- **THEN** the returned error identifies the source scheme, the operation, and
  the URI (e.g. `gs fetch "gs://bucket/x": <cause>`)

### Requirement: Behavior is preserved
Introducing the registry SHALL NOT change any user-facing behavior: the same
URIs resolve to the same results as before.

#### Scenario: Existing URIs still work
- **WHEN** the registry replaces the hard-coded dispatch
- **THEN** every previously-supported source URI continues to resolve and query
  identically (verified by the unit and E2E suites)

