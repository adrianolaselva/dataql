<p align="center">
  <img src="img/dataql.png" alt="DataQL" width="200">
</p>

# Auto-Issue Reporter

DataQL includes a skill that automatically creates GitHub issues when errors occur, with built-in duplicate validation to prevent issue spam.

## Overview

The `dataql-auto-issue` skill teaches Claude Code to:

1. **Detect failures** - Recognize when DataQL commands fail
2. **Validate duplicates** - Search existing issues before creating new ones
3. **Create issues** - Open well-formatted GitHub issues with all relevant information
4. **Add context** - Comment on existing issues if duplicates are found

## Installation

Install the auto-issue skill using the DataQL CLI:

```bash
# Interactive installation
dataql skills install

# Install globally (available in all projects)
dataql skills install --global

# Install for current project only
dataql skills install --project
```

## Prerequisites

The auto-issue feature requires:

1. **GitHub CLI (gh)** - Install from https://cli.github.com/
2. **Authentication** - Run `gh auth login` to authenticate

## How It Works

### Automatic Issue Creation

When a DataQL command fails, Claude will:

1. **Analyze the error** - Determine if it's a bug (not user error)
2. **Search for duplicates** - Query existing issues with similar keywords
3. **Create or comment** - Either create a new issue or add context to existing one

### Duplicate Detection

The skill uses multiple strategies to detect duplicates:

```bash
# Search by error keywords
gh issue list --repo adrianolaselva/dataql --state all --search "type inference column mismatch"

# Search by component
gh issue list --repo adrianolaselva/dataql --state all --search "csv parsing delimiter"
```

### Issue Format

New issues follow a standard format:

```markdown
## Description
Brief description of the issue

## Error Message
```
exact error message
```

## Steps to Reproduce
1. Step 1
2. Step 2
3. Step 3

## Expected Behavior
What should have happened

## Actual Behavior
What actually happened

## Environment
- DataQL Version: v1.0.0
- OS: Linux 6.6.87.2-microsoft-standard-WSL2
- Input File Type: CSV

## Command Executed
```bash
dataql run -f data.csv -q "SELECT * FROM data"
```

---
*This issue was auto-reported by Claude Code*
```

## Labels

Issues are automatically labeled based on the error type:

| Label | Description |
|-------|-------------|
| `bug` | Default for all errors |
| `auto-reported` | Indicates automated reporting |
| `file-handler` | CSV, JSON, Parquet handler issues |
| `database` | PostgreSQL, MySQL, MongoDB issues |
| `storage` | DuckDB storage layer issues |
| `mcp` | MCP server integration issues |
| `repl` | Interactive mode issues |
| `export` | Data export issues |

## Commands

### Create Issue Manually

Use the `/project:dataql-issue` command to create issues manually:

```
/project:dataql-issue "CSV parsing fails with special characters"
```

This will:
1. Search for existing similar issues
2. Show potential duplicates if found
3. Ask for confirmation before creating

## Configuration

The skill uses the DataQL repository by default:

- **Repository**: `adrianolaselva/dataql`
- **Default labels**: `bug`, `auto-reported`

## When Issues Are NOT Created

The skill avoids creating issues for:

- **User errors** - Wrong file paths, invalid SQL syntax
- **Missing dependencies** - User's system configuration
- **Network issues** - Temporary connectivity problems
- **Permission errors** - File access issues
- **Expected behavior** - Features working as designed

## Programmatic API

DataQL also provides a Go package for programmatic issue management:

```go
package main

import (
    "fmt"
    "github.com/adrianolaselva/dataql/pkg/github"
)

func main() {
    client := github.NewDefaultClient()

    // Check for duplicates
    duplicate, err := client.IsDuplicate(
        "CSV parsing fails",
        "failed to parse CSV: invalid delimiter",
    )
    if err != nil {
        panic(err)
    }

    if duplicate != nil {
        fmt.Printf("Duplicate found: #%d - %s\n", duplicate.Number, duplicate.Title)

        // Add comment to existing issue
        client.AddComment(duplicate.Number, "Additional occurrence reported...")
        return
    }

    // Create new issue
    issue, err := client.CreateIssue(github.IssueCreateRequest{
        Title:  "[Bug] CSV parsing fails with special characters",
        Body:   "## Description\n...",
        Labels: []string{github.LabelBug, github.LabelAutoReported, github.LabelFileHandler},
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Created issue: %s\n", issue.URL)
}
```

## Best Practices

1. **Include version info** - Always report DataQL version
2. **Exact error messages** - Copy verbatim, don't paraphrase
3. **Minimal reproduction** - Simplest steps to reproduce
4. **No sensitive data** - Don't include passwords, API keys, etc.
5. **Check authentication** - Ensure `gh auth status` shows logged in

## Troubleshooting

### gh CLI Not Installed

```
Error: gh CLI is not installed
```

Solution: Install from https://cli.github.com/

### Not Authenticated

```
Error: gh auth status failed
```

Solution: Run `gh auth login`

### Rate Limited

```
Error: API rate limit exceeded
```

Solution: Wait a few minutes and retry

## See Also

- [Getting Started](getting-started.md)
- [CLI Reference](cli-reference.md)
- [Skills Management](skills.md)
