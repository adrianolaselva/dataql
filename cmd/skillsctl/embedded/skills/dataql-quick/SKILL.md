---
name: dataql-quick
description: Quick data queries and previews. Use when user wants to see contents of a data file, check schema, or do simple filtering on CSV, JSON, or other data files.
tools:
  - Bash
---

# DataQL Quick Query

For fast data inspection and simple queries using DataQL.

## Quick Commands

| Task | Command |
|------|---------|
| Preview file | `dataql run -f <file> -q "SELECT * FROM <table> LIMIT 5"` |
| Count rows | `dataql run -f <file> -q "SELECT COUNT(*) FROM <table>"` |
| Check schema | `dataql run -f <file> -q ".schema <table>"` |
| List tables | `dataql run -f <file> -q ".tables"` |
| Distinct values | `dataql run -f <file> -q "SELECT DISTINCT <column> FROM <table>"` |
| Filter rows | `dataql run -f <file> -q "SELECT * FROM <table> WHERE <condition> LIMIT 10"` |

## File Naming Convention

- Table name = filename without extension
- `users.csv` -> table name is `users`
- `orders.json` -> table name is `orders`
- `data.parquet` -> table name is `data`

## Examples

### Preview a CSV file
```bash
dataql run -f users.csv -q "SELECT * FROM users LIMIT 5"
```

### Count records
```bash
dataql run -f orders.json -q "SELECT COUNT(*) as total FROM orders"
```

### Check structure
```bash
dataql run -f data.parquet -q ".schema data"
```

### Simple filter
```bash
dataql run -f products.csv -q "SELECT name, price FROM products WHERE price > 100 LIMIT 10"
```

### Read from stdin
```bash
cat data.csv | dataql run -f - -q "SELECT * FROM stdin_data LIMIT 5"
```

**Note:** When reading from stdin, the default table name is `stdin_data`.

## Supported Formats

- CSV (with custom delimiter: `-d ";"`)
- JSON (arrays or objects)
- JSONL/NDJSON
- XML
- YAML
- Parquet
- Excel (.xlsx, .xls)
- Avro
- ORC

## Output Options

- Default: formatted table
- JSON output: pipe to `jq` or use export
- CSV export: `-e output.csv -t csv`
- JSONL export: `-e output.jsonl -t jsonl`

## Notes

- Always use LIMIT for large files to avoid overwhelming output
- Use `.schema` first to understand column names and types
- For stdin input with non-CSV format: `-i json` or `-i jsonl`
