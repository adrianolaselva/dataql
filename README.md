# DataQL

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://go.dev)
[![Build](https://github.com/adrianolaselva/dataql/actions/workflows/build.yml/badge.svg)](https://github.com/adrianolaselva/dataql/actions/workflows/build.yml)
[![CI](https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml/badge.svg)](https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/adrianolaselva/dataql)](https://goreportcard.com/report/github.com/adrianolaselva/dataql)
[![GoDoc](https://godoc.org/github.com/adrianolaselva/dataql?status.svg)](https://pkg.go.dev/github.com/adrianolaselva/dataql)
![GitHub issues](https://img.shields.io/github/issues/adrianolaselva/dataql)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A powerful CLI tool for querying and transforming data across multiple formats

DataQL is a CLI tool developed in Go that allows you to query and manipulate data files using SQL statements.
It loads data into an SQLite database (in-memory or file-based) enabling powerful SQL operations on your data.

## Features

**Supported File Formats:**
- CSV (with configurable delimiter)
- JSON (arrays or single objects)
- JSONL/NDJSON (newline-delimited JSON)
- XML
- YAML
- Parquet
- Excel (.xlsx, .xls)
- Avro
- ORC

**Data Sources:**
- Local files
- HTTP/HTTPS URLs
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- Standard input (stdin)

**Database Connectors:**
- PostgreSQL
- MySQL
- DuckDB
- MongoDB

**Key Capabilities:**
- Execute SQL queries using SQLite syntax
- Export results to CSV or JSONL formats
- Interactive REPL mode with command history
- Progress bar for large file operations
- Parallel file processing for multiple inputs
- Automatic flattening of nested JSON objects
- Join data from multiple sources

## Installation

### Quick Install (Recommended)

**Linux / macOS:**

```bash
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
```

### Install Options

**Specific version:**

```bash
# Linux/macOS
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --version v1.0.0

# Windows
$env:DATAQL_VERSION="v1.0.0"; irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
```

**User installation (no sudo/admin required):**

```bash
# Linux/macOS
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --local

# Windows
$env:DATAQL_USER_INSTALL="true"; irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
```

### From Source

```bash
# Clone the repository
git clone https://github.com/adrianolaselva/dataql.git
cd dataql

# Build and install
make build
make install       # requires sudo
# or
make install-local # installs to ~/.local/bin
```

### Verify Installation

```bash
dataql --version
```

### Uninstall

**Linux / macOS:**

```bash
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/uninstall.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/uninstall.ps1 | iex
```

## Usage

### Basic Usage

Load a data file and start interactive mode (format is auto-detected):

```bash
# CSV file
dataql run -f data.csv -d ","

# JSON file (array or single object)
dataql run -f data.json

# JSONL/NDJSON file (one JSON per line)
dataql run -f data.jsonl
```

### Supported Input Formats

| Format | Extensions | Description |
|--------|------------|-------------|
| CSV | `.csv` | Comma-separated values with configurable delimiter |
| JSON | `.json` | JSON arrays or single objects |
| JSONL | `.jsonl`, `.ndjson` | Newline-delimited JSON (streaming) |
| XML | `.xml` | XML documents |
| YAML | `.yaml`, `.yml` | YAML documents |
| Parquet | `.parquet` | Apache Parquet columnar format |
| Excel | `.xlsx`, `.xls` | Microsoft Excel spreadsheets |
| Avro | `.avro` | Apache Avro format |
| ORC | `.orc` | Apache ORC format |

### Supported Data Sources

| Source | Format | Example |
|--------|--------|---------|
| Local file | Path | `-f data.csv` |
| HTTP/HTTPS | URL | `-f "https://example.com/data.csv"` |
| Amazon S3 | `s3://` | `-f "s3://bucket/path/data.csv"` |
| Google Cloud Storage | `gs://` | `-f "gs://bucket/path/data.json"` |
| Azure Blob | `az://` | `-f "az://container/path/data.parquet"` |
| Standard input | `-` | `cat data.csv \| dataql run -f -` |
| PostgreSQL | `postgres://` | `-f "postgres://user:pass@host/db?table=t"` |
| MySQL | `mysql://` | `-f "mysql://user:pass@host/db?table=t"` |
| DuckDB | `duckdb://` | `-f "duckdb:///path/db.db?table=t"` |
| MongoDB | `mongodb://` | `-f "mongodb://host/db?collection=c"` |

### Command Line Options

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--file` | `-f` | Input file, URL, or database connection | Required |
| `--delimiter` | `-d` | CSV delimiter (only for CSV files) | `,` |
| `--query` | `-q` | SQL query to execute | - |
| `--export` | `-e` | Export path | - |
| `--type` | `-t` | Export format (`csv`, `jsonl`) | - |
| `--storage` | `-s` | SQLite file path (for persistence) | In-memory |
| `--lines` | `-l` | Limit number of lines/records to read | All |
| `--collection` | `-c` | Custom table name | Filename |

### Examples

**Interactive Mode:**

```bash
dataql run -f sales.csv -d ";"
```

```
dataql> SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC LIMIT 10;
product      total
Widget Pro   125430.50
Gadget Plus   98210.00
...
```

**Execute Query and Display Results:**

```bash
dataql run -f data.csv -d "," -q "SELECT * FROM data WHERE amount > 100 LIMIT 10"
```

**Export to JSONL:**

```bash
dataql run -f input.csv -d "," \
  -q "SELECT id, name, value FROM input WHERE status = 'active'" \
  -e output.jsonl -t jsonl
```

**Export to CSV:**

```bash
dataql run -f input.csv -d "," \
  -q "SELECT * FROM input" \
  -e output.csv -t csv
```

**Multiple Input Files:**

```bash
dataql run -f file1.csv -f file2.csv -d "," \
  -q "SELECT a.*, b.extra FROM file1 a JOIN file2 b ON a.id = b.id"
```

**Query JSON Files:**

```bash
# JSON array
dataql run -f users.json -q "SELECT name, email FROM users WHERE status = 'active'"

# JSON with nested objects (automatically flattened)
# {"user": {"name": "John", "address": {"city": "NYC"}}}
# becomes columns: user_name, user_address_city
dataql run -f data.json -q "SELECT user_name, user_address_city FROM data"
```

**Query JSONL/NDJSON Files:**

```bash
# JSONL is ideal for large datasets (streaming, low memory)
dataql run -f logs.jsonl -q "SELECT level, message, timestamp FROM logs WHERE level = 'ERROR'"

# Works with .ndjson extension too
dataql run -f events.ndjson -q "SELECT COUNT(*) as total FROM events"
```

**Custom Table Name:**

```bash
# Use --collection to specify a custom table name
dataql run -f data.json -c my_table -q "SELECT * FROM my_table"
```

**Persist to SQLite File:**

```bash
dataql run -f data.csv -d "," -s ./database.db
```

**Query from URL:**

```bash
dataql run -f "https://raw.githubusercontent.com/datasets/population/main/data/population.csv" \
  -q "SELECT Country_Name, Value FROM population WHERE Year = 2020 LIMIT 10"
```

**Query from S3:**

```bash
dataql run -f "s3://my-bucket/data/sales.csv" \
  -q "SELECT product, SUM(amount) as total FROM sales GROUP BY product"
```

**Query from PostgreSQL:**

```bash
dataql run -f "postgres://user:pass@localhost:5432/mydb?table=orders" \
  -q "SELECT * FROM orders WHERE status = 'completed'"
```

**Read from stdin:**

```bash
cat data.csv | dataql run -f - -q "SELECT * FROM stdin WHERE value > 100"
```

### Real-World Example

```bash
# Download sample data
wget https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv -O survey.csv

# Query and export
dataql run -f survey.csv -d "," \
  -q "SELECT Year, Industry_aggregation_NZSIOC as industry, Variable_name as metric, Value as amount FROM survey WHERE Value > 1000" \
  -e analysis.jsonl -t jsonl
```

## SQL Reference

DataQL uses SQLite under the hood, supporting standard SQL syntax:

```sql
-- Basic SELECT
SELECT column1, column2 FROM tablename;

-- Filtering
SELECT * FROM data WHERE amount > 100 AND status = 'active';

-- Aggregation
SELECT category, COUNT(*), SUM(value) FROM data GROUP BY category;

-- Joins (multiple files)
SELECT a.*, b.extra FROM file1 a JOIN file2 b ON a.id = b.id;

-- Ordering and Limiting
SELECT * FROM data ORDER BY created_at DESC LIMIT 100;
```

> **Note:** Table names are derived from filenames (without extension). For `sales.csv`, `sales.json`, or `sales.jsonl`, use `SELECT * FROM sales`. Use `--collection` flag to specify a custom table name.

## Documentation

For detailed documentation, see:

- [Getting Started](docs/getting-started.md) - Installation and Hello World examples
- [CLI Reference](docs/cli-reference.md) - Complete command-line reference
- [Data Sources](docs/data-sources.md) - Working with S3, GCS, Azure, URLs, and stdin
- [Database Connections](docs/databases.md) - Connect to PostgreSQL, MySQL, DuckDB, MongoDB
- [Examples](docs/examples.md) - Real-world usage examples and automation scripts

## Development

### Prerequisites

- Go 1.24 or higher
- GCC (for CGO compilation - required for SQLite, DuckDB)

### Building

```bash
make build
```

### Testing

```bash
make test
```

### Linting

```bash
make lint
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [SQLite](https://www.sqlite.org/) - Embedded database engine
- [Cobra](https://github.com/spf13/cobra) - CLI framework
- [go-sqlite3](https://github.com/mattn/go-sqlite3) - SQLite driver for Go
