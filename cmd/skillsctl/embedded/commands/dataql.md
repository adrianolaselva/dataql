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
