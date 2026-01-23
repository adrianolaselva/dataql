---
description: Execute DataQL queries on data files
---

# DataQL Query

Execute a SQL query on a data file using DataQL.

## Usage

/project:dataql <file> <query>

## Arguments

- `file`: Path to the data file (CSV, JSON, Parquet, etc.) or URL
- `query`: SQL query to execute

## Examples

```
/project:dataql users.csv "SELECT * FROM users WHERE age > 30"
/project:dataql sales.json "SELECT product, SUM(amount) FROM sales GROUP BY product"
/project:dataql "s3://bucket/data.parquet" "SELECT * FROM data LIMIT 10"
```

## Instructions

1. Parse the file path and query from the arguments
2. Execute: `dataql run -f <file> -q "<query>"`
3. Display the results in a formatted table
4. If the query fails, show the error and suggest corrections

## Error Handling

- If file not found: suggest checking the path
- If SQL syntax error: show the error and suggest fixes
- If column not found: run `.schema` to show available columns

## Advanced Options

- `--cache`: Enable caching for faster repeated queries
- `--cache-dir <dir>`: Specify custom cache directory
- `-p key=value`: Pass query parameters (use `:key` in SQL)
- `-v`: Verbose mode for debugging
- `-Q`: Quiet mode (suppress progress bar)
- `--vertical`: Display results vertically (like MySQL \G)
- `--truncate <n>`: Truncate columns at n characters

## Useful Patterns

```bash
# Parameterized queries
dataql run -f users.csv -q "SELECT * FROM users WHERE status = :status" -p status=active

# Cache for large files
dataql run -f large.csv -q "SELECT COUNT(*) FROM large" --cache

# Export results
dataql run -f data.csv -q "SELECT * FROM data WHERE x > 10" -e output.csv -t csv

# Describe data
dataql describe -f data.csv
```
