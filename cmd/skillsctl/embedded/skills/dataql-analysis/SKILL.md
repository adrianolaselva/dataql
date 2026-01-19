---
name: dataql-analysis
description: Analyze data files using SQL queries with DataQL. Use when working with CSV, JSON, Parquet, Excel files or when the user mentions data analysis, filtering, aggregation, or SQL queries on files.
tools:
  - Bash
---

# DataQL Data Analysis

You have access to DataQL, a powerful CLI tool for querying data files using SQL.

## Capabilities

- Query CSV, JSON, JSONL, XML, YAML, Parquet, Excel, Avro, ORC files
- Filter, aggregate, join data from multiple sources
- Export results to CSV or JSONL
- Connect to databases (PostgreSQL, MySQL, MongoDB)
- Query data from S3, GCS, Azure Blob Storage, and HTTP URLs

## Usage Patterns

### Single File Query
```bash
dataql run -f <file> -q "<SQL query>"
```

### Multiple Files (JOIN)
```bash
dataql run -f users.csv -f orders.json -q "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
```

### Export Results
```bash
dataql run -f data.csv -q "SELECT * FROM data WHERE amount > 100" -e output.jsonl -t jsonl
```

### Get Schema
```bash
dataql run -f data.csv -q ".schema data"
```

### Query from URL
```bash
dataql run -f "https://example.com/data.json" -q "SELECT * FROM data LIMIT 10"
```

### Query from S3
```bash
dataql run -f "s3://bucket/path/data.csv" -q "SELECT * FROM data"
```

### Query from Database
```bash
dataql run -f "postgres://user:pass@host/db?table=users" -q "SELECT * FROM users WHERE active = true"
```

## Best Practices for Token Efficiency

1. **Always use LIMIT**: Start with `LIMIT 10` to preview data before running full queries
2. **Select specific columns**: Avoid `SELECT *` when possible - specify only needed columns
3. **Use aggregations**: `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()` instead of returning all rows
4. **Filter early**: Use WHERE clauses to reduce result size
5. **Check schema first**: Run `.schema` before complex queries to understand structure

## Common Workflow

### Step 1: Understand the data structure
```bash
dataql run -f data.csv -q ".schema data"
```

### Step 2: Preview a few rows
```bash
dataql run -f data.csv -q "SELECT * FROM data LIMIT 5"
```

### Step 3: Get summary statistics
```bash
dataql run -f data.csv -q "SELECT COUNT(*) as total, AVG(amount) as avg_amount FROM data"
```

### Step 4: Run targeted queries
```bash
dataql run -f data.csv -q "SELECT category, SUM(amount) as total FROM data GROUP BY category ORDER BY total DESC LIMIT 10"
```

## Examples

### Analyze Sales Data
```bash
# Check schema
dataql run -f sales.csv -q ".schema sales"

# Get summary
dataql run -f sales.csv -q "SELECT COUNT(*) as transactions, SUM(amount) as revenue, AVG(amount) as avg_sale FROM sales"

# Top products
dataql run -f sales.csv -q "SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC LIMIT 10"

# Sales by date
dataql run -f sales.csv -q "SELECT date, SUM(amount) as daily_total FROM sales GROUP BY date ORDER BY date"
```

### Join Multiple Sources
```bash
dataql run -f customers.csv -f orders.json -q "
SELECT c.name, c.email, COUNT(o.id) as order_count, SUM(o.total) as total_spent
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 20"
```

### Filter and Export
```bash
# Find and export active users
dataql run -f users.json -q "SELECT id, name, email FROM users WHERE status = 'active'" -e active_users.csv -t csv

# Export as JSONL for further processing
dataql run -f data.parquet -q "SELECT * FROM data WHERE region = 'US'" -e us_data.jsonl -t jsonl
```

## Notes

- Table name defaults to filename without extension (e.g., `users.csv` -> `users`)
- Use `-c` flag to specify custom table name: `dataql run -f data.csv -c mytable -q "SELECT * FROM mytable"`
- For stdin: use `-f -` and table name is `stdin`
- Use `-v` for verbose output with detailed logging
