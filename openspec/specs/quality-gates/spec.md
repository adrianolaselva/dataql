# quality-gates Specification

## Purpose
TBD - created by archiving change quality-harness-and-modernization. Update Purpose after archive.
## Requirements
### Requirement: Coverage ratchet
CI SHALL record a committed test-coverage baseline and MUST fail any pull
request whose total coverage falls below that baseline. When coverage rises, the
baseline MAY be raised but MUST never be lowered. The long-term target is
>= 90% total coverage.

#### Scenario: Coverage drops below baseline
- **WHEN** a pull request reduces total coverage below the recorded baseline
- **THEN** the CI coverage check fails and the PR cannot be merged

#### Scenario: Coverage rises
- **WHEN** a pull request increases total coverage above the recorded baseline
- **THEN** CI passes and the baseline can be updated to the new, higher value

### Requirement: Static analysis gate
CI SHALL run the project linters and, for the linters designated as blocking,
MUST fail the build on any reported issue. The set of blocking linters MUST only
grow over time (a linter, once blocking, is not silently disabled).

#### Scenario: Blocking linter finds an issue
- **WHEN** code violates a rule from a blocking linter
- **THEN** the lint CI job fails

### Requirement: Vulnerability scan gate
CI SHALL run `govulncheck` against the codebase and dependencies and MUST fail
the build when a known vulnerability affecting a used code path is found.

#### Scenario: Vulnerable dependency in a used path
- **WHEN** a dependency with a known vulnerability that DataQL actually calls is
  present
- **THEN** the govulncheck CI job fails

### Requirement: Automated dependency freshness
The project SHALL run an automated dependency-update mechanism that opens update
proposals for Go modules and the toolchain, keeping the dependency tree current
after this change.

#### Scenario: A dependency has a newer release
- **WHEN** a tracked dependency publishes a newer compatible version
- **THEN** the automation opens an update proposal that runs the full CI gate

