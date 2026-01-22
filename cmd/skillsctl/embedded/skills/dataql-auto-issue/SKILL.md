---
name: dataql-auto-issue
description: Automatically create GitHub issues when DataQL errors occur, with duplicate validation. Use when DataQL commands fail or produce unexpected errors.
tools:
  - Bash
---

# DataQL Auto-Issue Reporter

You have the ability to automatically create GitHub issues when DataQL commands fail. This ensures errors are tracked and helps improve the tool.

## When to Create Issues

Create an issue when:
- DataQL command fails with an error
- Unexpected behavior occurs
- A bug is reproducible
- The error message suggests a code problem (not user error)

Do NOT create issues for:
- User errors (wrong file path, invalid SQL syntax user typed)
- Missing dependencies on user's system
- Network/permission issues
- Expected behavior that user misunderstands

## Duplicate Validation (REQUIRED)

**ALWAYS check for existing issues before creating a new one:**

### Step 1: Search for similar issues
```bash
gh issue list --repo adrianolaselva/dataql --state all --search "<error keywords>" --limit 20
```

### Step 2: Check if duplicate exists
Look for issues with:
- Similar error messages
- Same component/feature affected
- Similar reproduction steps

### Step 3: If duplicate found
- Add a comment to the existing issue with additional context
- Do NOT create a new issue

```bash
gh issue comment <issue_number> --repo adrianolaselva/dataql --body "Additional occurrence reported:
- Context: <context>
- Error: <error>
- Version: $(dataql --version 2>/dev/null || echo 'unknown')"
```

### Step 4: If no duplicate found
- Create a new issue with full details

## Issue Creation

### Get System Information
```bash
# Get DataQL version
dataql --version 2>/dev/null || echo "version unknown"

# Get OS info
uname -a

# Get Go version (if relevant)
go version 2>/dev/null || echo "go not found"
```

### Create Issue
```bash
gh issue create --repo adrianolaselva/dataql \
  --title "[Bug] <short description>" \
  --label "bug,auto-reported" \
  --body "$(cat <<'EOF'
## Description
<Brief description of the issue>

## Error Message
```
<exact error message>
```

## Steps to Reproduce
1. <step 1>
2. <step 2>
3. <step 3>

## Expected Behavior
<what should have happened>

## Actual Behavior
<what actually happened>

## Environment
- DataQL Version: <version>
- OS: <os info>
- Input File Type: <csv/json/etc>

## Command Executed
```bash
<the exact command that failed>
```

## Additional Context
<any relevant context>

---
*This issue was auto-reported by Claude Code*
EOF
)"
```

## Labels

Use appropriate labels:
- `bug` - For errors and unexpected behavior
- `auto-reported` - Indicates automated reporting
- `file-handler` - Issues with CSV, JSON, Parquet handlers
- `database` - Issues with PostgreSQL, MySQL, MongoDB connectors
- `storage` - Issues with DuckDB storage layer
- `mcp` - Issues with MCP server integration
- `repl` - Issues with interactive mode
- `export` - Issues with data export functionality

## Common Error Patterns

### File Handler Errors
```bash
# Search pattern
gh issue list --repo adrianolaselva/dataql --state all --search "file handler csv json parquet"
```

### Database Connection Errors
```bash
# Search pattern
gh issue list --repo adrianolaselva/dataql --state all --search "database connection postgres mysql mongodb"
```

### Storage/DuckDB Errors
```bash
# Search pattern
gh issue list --repo adrianolaselva/dataql --state all --search "duckdb storage insert"
```

### Type Inference Errors
```bash
# Search pattern
gh issue list --repo adrianolaselva/dataql --state all --search "type inference column mismatch"
```

## Example Workflow

### Error Occurs
```
$ dataql run -f data.csv -q "SELECT * FROM data"
Error: failed to infer column types: unexpected nil value in row 150
```

### Check for Duplicates
```bash
gh issue list --repo adrianolaselva/dataql --state all --search "infer column types nil value"
```

### No duplicate found - Create Issue
```bash
gh issue create --repo adrianolaselva/dataql \
  --title "[Bug] Type inference fails with nil value in middle of data" \
  --label "bug,auto-reported,storage" \
  --body "## Description
Type inference fails when a CSV file contains nil/empty values after row 100.

## Error Message
\`\`\`
Error: failed to infer column types: unexpected nil value in row 150
\`\`\`

## Steps to Reproduce
1. Create a CSV with 200 rows
2. Leave some cells empty in rows 150+
3. Run: dataql run -f data.csv -q \"SELECT * FROM data\"

## Expected Behavior
DataQL should handle nil values gracefully, using NULL in the database.

## Actual Behavior
Command fails with type inference error.

## Environment
- DataQL Version: v1.0.0
- OS: Linux 6.6.87.2-microsoft-standard-WSL2
- Input File Type: CSV

## Command Executed
\`\`\`bash
dataql run -f data.csv -q \"SELECT * FROM data\"
\`\`\`

---
*This issue was auto-reported by Claude Code*"
```

## Important Notes

1. **Always validate duplicates first** - This prevents issue spam
2. **Include version information** - Essential for debugging
3. **Provide exact error messages** - Copy verbatim, don't paraphrase
4. **Include reproduction steps** - Be specific and minimal
5. **Use appropriate labels** - Helps maintainers triage
6. **Respect user privacy** - Don't include sensitive data in issues

## Authentication

Ensure `gh` CLI is authenticated:
```bash
gh auth status
```

If not authenticated, prompt the user to authenticate:
```bash
gh auth login
```
