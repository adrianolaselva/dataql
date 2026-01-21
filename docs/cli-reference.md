<p align="center">
  <img src="img/dataql.png" alt="DataQL" width="200">
</p>

# CLI Reference

Complete command-line reference for DataQL.

## Commands

### `dataql run`

The main command to load data and execute queries.

```bash
dataql run [flags]
```

## Flags

| Flag | Short | Description | Default | Required |
|------|-------|-------------|---------|----------|
| `--file` | `-f` | Input file path, URL, or `-` for stdin | - | Yes |
| `--query` | `-q` | SQL query to execute | - | No |
| `--delimiter` | `-d` | CSV field delimiter | `,` | No |
| `--export` | `-e` | Export results to file path | - | No |
| `--type` | `-t` | Export format (`csv`, `jsonl`, `json`, `xml`, `yaml`, `excel`, `parquet`) | - | No |
| `--storage` | `-s` | DuckDB file path for persistence | In-memory | No |
| `--lines` | `-l` | Limit number of records to read | All | No |
| `--collection` | `-c` | Custom table name | Filename | No |

## Global Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--help` | `-h` | Display help information |
| `--version` | `-v` | Display version information |

## Input Sources

### Local Files

```bash
# CSV file
dataql run -f /path/to/data.csv

# JSON file
dataql run -f /path/to/data.json

# JSONL file
dataql run -f /path/to/data.jsonl

# XML file
dataql run -f /path/to/data.xml

# YAML file
dataql run -f /path/to/data.yaml

# Parquet file
dataql run -f /path/to/data.parquet

# Excel file
dataql run -f /path/to/data.xlsx
```

### URLs

```bash
# HTTP/HTTPS URL
dataql run -f "https://example.com/data.csv" -q "SELECT * FROM data"
```

### Standard Input (stdin)

```bash
# Pipe data from other commands
cat data.csv | dataql run -f - -q "SELECT * FROM stdin"

# Pipe JSON
echo '[{"a":1},{"a":2}]' | dataql run -f - -q "SELECT * FROM stdin"
```

### Cloud Storage

```bash
# Amazon S3
dataql run -f "s3://bucket-name/path/to/data.csv"

# Google Cloud Storage
dataql run -f "gs://bucket-name/path/to/data.json"

# Azure Blob Storage
dataql run -f "az://container/path/to/data.parquet"
```

### Databases

```bash
# PostgreSQL
dataql run -f "postgres://user:pass@host:5432/database?table=users"

# MySQL
dataql run -f "mysql://user:pass@host:3306/database?table=orders"

# DuckDB
dataql run -f "duckdb:///path/to/database.db?table=data"

# MongoDB
dataql run -f "mongodb://user:pass@host:27017/database?collection=documents"

# DynamoDB
dataql run -f "dynamodb://us-east-1/table-name"
dataql run -f "dynamodb://us-east-1/table-name?endpoint=http://localhost:4566"
```

## Supported File Formats

| Format | Extensions | Description |
|--------|------------|-------------|
| CSV | `.csv` | Comma-separated values (configurable delimiter) |
| JSON | `.json` | JSON arrays or single objects |
| JSONL | `.jsonl`, `.ndjson` | Newline-delimited JSON |
| XML | `.xml` | XML documents |
| YAML | `.yaml`, `.yml` | YAML documents |
| Parquet | `.parquet` | Apache Parquet columnar format |
| Excel | `.xlsx`, `.xls` | Microsoft Excel spreadsheets |
| Avro | `.avro` | Apache Avro format |
| ORC | `.orc` | Apache ORC format |

## Interactive Mode (REPL)

When you run DataQL without a query, it starts in interactive mode:

```bash
dataql run -f data.csv
```

### REPL Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all loaded tables |
| `.schema [table]` | Show schema for a table |
| `.count [table]` | Count rows in a table |
| `.help` | Show available commands |
| `.exit` or `.quit` | Exit the REPL |
| `.clear` | Clear the screen |
| `.version` | Show DataQL version |
| `.paging [on\|off]` | Toggle paged output for large results |
| `.pagesize [n]` | Set number of rows per page (default: 20) |
| `.timing [on\|off]` | Toggle query execution timing |
| `Ctrl+C` | Cancel current query |
| `Ctrl+D` | Exit the REPL |

### REPL Features

- **Command History**: Use arrow keys to navigate through previous commands
- **Multi-line Queries**: Continue queries across multiple lines
- **Tab Completion**: Auto-complete table names, column names, and SQL keywords
- **Syntax Highlighting**: SQL keywords are highlighted for readability
- **Paged Output**: Large results are paginated for easier navigation

## Usage Examples

### Basic Query

```bash
dataql run -f users.csv -q "SELECT * FROM users WHERE active = 1"
```

### Query with Custom Delimiter

```bash
dataql run -f data.csv -d ";" -q "SELECT * FROM data"
```

### Limit Records

```bash
dataql run -f large_file.csv -l 1000 -q "SELECT * FROM large_file"
```

### Custom Table Name

```bash
dataql run -f data.csv -c my_table -q "SELECT * FROM my_table"
```

### Export to CSV

```bash
dataql run -f input.json -q "SELECT * FROM input" -e output.csv -t csv
```

### Export to JSONL

```bash
dataql run -f input.csv -q "SELECT * FROM input" -e output.jsonl -t jsonl
```

### Export to JSON

```bash
dataql run -f input.csv -q "SELECT * FROM input" -e output.json -t json
```

### Export to Excel

```bash
dataql run -f input.csv -q "SELECT * FROM input" -e output.xlsx -t excel
```

### Export to Parquet

```bash
dataql run -f input.csv -q "SELECT * FROM input" -e output.parquet -t parquet
```

### Export to XML

```bash
dataql run -f input.csv -q "SELECT * FROM input" -e output.xml -t xml
```

### Export to YAML

```bash
dataql run -f input.csv -q "SELECT * FROM input" -e output.yaml -t yaml
```

### Multiple Input Files

```bash
dataql run -f orders.csv -f customers.csv -q "
SELECT o.*, c.name as customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
"
```

### Persist to DuckDB File

```bash
dataql run -f data.csv -s ./my_database.duckdb
```

### Query from URL

```bash
dataql run -f "https://raw.githubusercontent.com/datasets/population/main/data/population.csv" \
  -q "SELECT Country_Name, Year, Value FROM population WHERE Year = 2020 LIMIT 10"
```

### Pipe from curl

```bash
curl -s "https://api.example.com/data.json" | dataql run -f - -q "SELECT * FROM stdin"
```

## SQL Reference

DataQL uses DuckDB under the hood. All standard DuckDB SQL syntax is supported, optimized for analytical queries (OLAP).

### SELECT

```sql
SELECT column1, column2 FROM table_name;
SELECT * FROM table_name;
SELECT DISTINCT column FROM table_name;
```

### WHERE

```sql
SELECT * FROM data WHERE amount > 100;
SELECT * FROM data WHERE status = 'active' AND age >= 18;
SELECT * FROM data WHERE name LIKE 'John%';
SELECT * FROM data WHERE id IN (1, 2, 3);
SELECT * FROM data WHERE email IS NOT NULL;
```

### ORDER BY

```sql
SELECT * FROM data ORDER BY created_at DESC;
SELECT * FROM data ORDER BY category ASC, amount DESC;
```

### LIMIT and OFFSET

```sql
SELECT * FROM data LIMIT 10;
SELECT * FROM data LIMIT 10 OFFSET 20;
```

### GROUP BY

```sql
SELECT category, COUNT(*) as count FROM data GROUP BY category;
SELECT category, SUM(amount), AVG(price) FROM data GROUP BY category;
```

### HAVING

```sql
SELECT category, COUNT(*) as count
FROM data
GROUP BY category
HAVING count > 5;
```

### JOIN

```sql
-- INNER JOIN
SELECT a.*, b.extra
FROM table1 a
JOIN table2 b ON a.id = b.foreign_id;

-- LEFT JOIN
SELECT a.*, b.extra
FROM table1 a
LEFT JOIN table2 b ON a.id = b.foreign_id;
```

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count rows |
| `COUNT(column)` | Count non-null values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average of values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |
| `GROUP_CONCAT(column)` | Concatenate values |

### String Functions

| Function | Description |
|----------|-------------|
| `LENGTH(str)` | String length |
| `UPPER(str)` | Convert to uppercase |
| `LOWER(str)` | Convert to lowercase |
| `TRIM(str)` | Remove leading/trailing spaces |
| `SUBSTR(str, start, len)` | Extract substring |
| `REPLACE(str, old, new)` | Replace occurrences |
| `INSTR(str, substr)` | Find position of substring |

### Date/Time Functions

| Function | Description |
|----------|-------------|
| `DATE(value)` | Extract date |
| `TIME(value)` | Extract time |
| `DATETIME(value)` | Date and time |
| `STRFTIME(format, value)` | Format date/time |
| `JULIANDAY(value)` | Julian day number |

### Type Conversion

```sql
SELECT CAST(amount AS INTEGER) FROM data;
SELECT CAST(price AS REAL) FROM data;
SELECT CAST(id AS TEXT) FROM data;
```

## Environment Variables

### Cloud Storage

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key for S3/SQS |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key for S3/SQS |
| `AWS_REGION` | AWS region for S3/SQS |
| `AWS_ENDPOINT_URL` | Custom endpoint (for LocalStack, MinIO) |
| `AWS_ENDPOINT_URL_S3` | S3-specific custom endpoint |
| `AWS_ENDPOINT_URL_SQS` | SQS-specific custom endpoint |
| `AWS_ENDPOINT_URL_DYNAMODB` | DynamoDB-specific custom endpoint |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account JSON |
| `AZURE_STORAGE_ACCOUNT` | Azure storage account name |
| `AZURE_STORAGE_KEY` | Azure storage account key |

### Message Queues

| Variable | Description |
|----------|-------------|
| `KAFKA_BROKERS` | Kafka bootstrap servers (comma-separated) |
| `KAFKA_SASL_USERNAME` | Kafka SASL username |
| `KAFKA_SASL_PASSWORD` | Kafka SASL password |
| `KAFKA_SASL_MECHANISM` | Kafka SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |
| 3 | File not found |
| 4 | Connection error |
| 5 | Query error |

## See Also

- [Getting Started](getting-started.md)
- [Data Sources](data-sources.md)
- [Database Connections](databases.md)
- [Examples](examples.md)
