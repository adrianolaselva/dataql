---
description: Create or find GitHub issues for DataQL errors with duplicate validation
---

# DataQL Issue Reporter

Create a GitHub issue for DataQL errors, with automatic duplicate detection.

## Usage

/project:dataql-issue <error_description>

## Arguments

- `error_description`: Brief description of the error or issue

## Examples

```
/project:dataql-issue "CSV parsing fails with special characters"
/project:dataql-issue "Type mismatch error when joining files"
/project:dataql-issue "MongoDB connection timeout"
```

## Instructions

### 1. Search for Existing Issues First

```bash
gh issue list --repo adrianolaselva/dataql --state all --search "<keywords from error>" --limit 20
```

### 2. If Similar Issue Found

- Show the existing issue(s) to the user
- Ask if they want to add a comment to an existing issue
- If yes, add a comment with the new context:

```bash
gh issue comment <number> --repo adrianolaselva/dataql --body "Additional report:
- Error: <error_description>
- Context: <context if available>"
```

### 3. If No Duplicate Found

Gather information and create the issue:

```bash
# Get version
DATAQL_VERSION=$(dataql --version 2>/dev/null || echo "unknown")

# Get OS
OS_INFO=$(uname -a)

# Create issue
gh issue create --repo adrianolaselva/dataql \
  --title "[Bug] <error_description>" \
  --label "bug" \
  --body "## Description
<error_description>

## Environment
- DataQL Version: $DATAQL_VERSION
- OS: $OS_INFO

## Additional Context
<any context from the conversation>

---
*Issue created via Claude Code*"
```

### 4. Confirm with User

Before creating, show the user:
- The issue title
- The labels
- A preview of the body

Ask for confirmation before submitting.

## Error Handling

- If `gh` is not installed: "Please install GitHub CLI: https://cli.github.com/"
- If not authenticated: "Please run: gh auth login"
- If repo not found: Verify the repository URL
- If rate limited: Wait and retry

## Labels Reference

Suggest appropriate labels based on the error:
- `bug` - Default for errors
- `enhancement` - Feature requests
- `documentation` - Doc issues
- `file-handler` - CSV/JSON/Parquet issues
- `database` - DB connection issues
- `storage` - DuckDB issues
- `mcp` - MCP server issues
- `repl` - Interactive mode issues
