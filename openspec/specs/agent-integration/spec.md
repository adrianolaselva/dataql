# agent-integration Specification

## Purpose
TBD - created by archiving change distribution-and-install. Update Purpose after archive.
## Requirements
### Requirement: One command configures all supported agents
DataQL SHALL provide a `dataql setup` command that registers the MCP server and
installs the skills for Claude Code, Codex, and opencode. The command MUST work
offline (skills are embedded; it only writes local configuration).

#### Scenario: Setup configures an installed agent
- **WHEN** a user runs `dataql setup` on a machine where a supported agent's
  config directory exists
- **THEN** the agent's configuration contains an MCP server entry that launches
  `dataql mcp serve`, and the DataQL skills are installed for that agent

#### Scenario: No supported agent present
- **WHEN** `dataql setup` runs and no supported agent is detected
- **THEN** it reports that none were configured and exits successfully without
  creating stray files

### Requirement: Setup is idempotent and non-destructive
Running `dataql setup` repeatedly SHALL be a no-op after the first run, and it
MUST merge into existing agent configuration without removing or overwriting
unrelated entries.

#### Scenario: Re-running setup
- **WHEN** `dataql setup` is run a second time
- **THEN** the configuration is unchanged and no duplicate MCP entry is created

#### Scenario: Existing unrelated config is preserved
- **WHEN** an agent config already contains other MCP servers or settings
- **THEN** after setup those entries remain intact and only the `dataql` entry is
  added or updated

### Requirement: Install configures agents automatically
The install script SHALL run `dataql setup` after installing the binary so that
agents work with no manual configuration step.

#### Scenario: Install then use with an agent
- **WHEN** a user installs DataQL via the script with a supported agent present
- **THEN** the agent can invoke DataQL via MCP without the user editing any
  config by hand

