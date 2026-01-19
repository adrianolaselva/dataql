---
description: Show schema of a data file
---

# DataQL Schema

Show the structure/schema of a data file using DataQL.

## Usage

/project:dataql-schema <file>

## Arguments

- `file`: Path to the data file (CSV, JSON, Parquet, etc.) or URL

## Examples

```
/project:dataql-schema users.csv
/project:dataql-schema orders.json
/project:dataql-schema "s3://bucket/data.parquet"
```

## Instructions

1. Determine the table name from the filename (without extension)
2. Execute: `dataql run -f <file> -q ".schema <tablename>"`
3. Display column names, types, and any constraints
4. If multiple files provided, show schema for each

## Output Format

The schema output will show:
- Column names
- Data types (TEXT, INTEGER, REAL, etc.)
- Nullable status
