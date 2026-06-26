<p align="center">
  <img src="docs/img/dataql.png" alt="DataQL Logo" width="400">
</p>

<h1 align="center">DataQL</h1>

<p align="center">
  <strong>Query any data file using SQL. One command, instant results.</strong>
</p>

<!-- Build & quality -->
<p align="center">
  <a href="https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml"><img src="https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/adrianolaselva/dataql/actions/workflows/build.yml"><img src="https://github.com/adrianolaselva/dataql/actions/workflows/build.yml/badge.svg" alt="Build"></a>
  <a href="https://app.codecov.io/gh/adrianolaselva/dataql"><img src="https://img.shields.io/codecov/c/github/adrianolaselva/dataql?logo=codecov&label=coverage" alt="Coverage"></a>
  <a href="https://goreportcard.com/report/github.com/adrianolaselva/dataql"><img src="https://goreportcard.com/badge/github.com/adrianolaselva/dataql" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/adrianolaselva/dataql"><img src="https://pkg.go.dev/badge/github.com/adrianolaselva/dataql.svg" alt="Go Reference"></a>
</p>

<!-- Distribution -->
<p align="center">
  <a href="https://github.com/adrianolaselva/dataql/releases/latest"><img src="https://img.shields.io/github/v/release/adrianolaselva/dataql?logo=github&label=release&sort=semver" alt="Latest release"></a>
  <a href="https://github.com/adrianolaselva/dataql/pkgs/container/dataql"><img src="https://img.shields.io/badge/ghcr.io-dataql-2496ED?logo=docker&logoColor=white" alt="Docker image"></a>
  <img src="https://img.shields.io/badge/platform-linux%20%7C%20macOS%20%7C%20windows-lightgrey" alt="Platforms">
  <a href="https://go.dev"><img src="https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
</p>

<!-- Project -->
<p align="center">
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-ready-8A2BE2?logo=anthropic&logoColor=white" alt="MCP ready"></a>
  <img src="https://img.shields.io/badge/offline-self--contained-success" alt="Self-contained / offline">
  <a href="https://github.com/adrianolaselva/dataql/stargazers"><img src="https://img.shields.io/github/stars/adrianolaselva/dataql?logo=github" alt="Stars"></a>
  <a href="https://github.com/adrianolaselva/dataql/commits/main"><img src="https://img.shields.io/github/last-commit/adrianolaselva/dataql" alt="Last commit"></a>
  <a href="https://github.com/adrianolaselva/dataql/issues"><img src="https://img.shields.io/github/issues/adrianolaselva/dataql" alt="GitHub issues"></a>
  <a href="CONTRIBUTING.md"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen" alt="PRs welcome"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#features">Features</a> •
  <a href="#installation">Installation</a> •
  <a href="#usage">Usage</a> •
  <a href="#llm-integration">LLM Integration</a> •
  <a href="#documentation">Documentation</a>
</p>

---

DataQL is a CLI tool developed in Go that allows you to query and manipulate data files using SQL statements.
It loads data into a DuckDB database (in-memory or file-based) with automatic type inference, enabling powerful SQL operations optimized for analytical queries.

## Why DataQL?

### The Problem

Working with data files has always been tedious. You either write throwaway scripts, load everything into pandas, or copy-paste into spreadsheets. With LLMs entering the workflow, a new problem emerged: **how do you analyze a 10MB CSV without burning through your entire context window?**

Traditional approaches fail:
- **Send file to LLM context**: 10MB CSV = ~100,000+ tokens. Expensive, slow, often impossible.
- **Write a script**: Context switch, setup overhead, not conversational.
- **Use pandas/Excel**: Great for humans, useless for LLM automation.

### The Solution

DataQL lets you query any data file using SQL. One command, instant results:

```bash
# Instead of sending 50,000 rows to an LLM...
dataql run -f sales.csv -q "SELECT region, SUM(revenue) FROM sales GROUP BY region"

# You get just what you need:
# region    | SUM(revenue)
# North     | 1,234,567
# South     | 987,654
```

### Why This Matters

| Scenario | Without DataQL | With DataQL |
|----------|---------------|-------------|
| Analyze 10MB CSV with LLM | ~100,000 tokens ($3+) | ~500 tokens ($0.01) |
| Query data from S3 | Download → Script → Parse | One command |
| Join CSV + JSON + Database | Custom ETL pipeline | Single SQL query |
| Automate data reports | Complex scripts | Simple CLI + cron |
| LLM data analysis | Context overflow | No size limit |

### Key Benefits

- **Token Efficient**: LLMs get query results, not raw data. 99% reduction in token usage.
- **Universal Format Support**: CSV, JSON, JSONL, Parquet, Excel, XML, YAML, Avro, ORC — all queryable with SQL (plus transparent `.gz`, `.bz2`, `.xz` decompression).
- **Any Data Source**: Local files, URLs, S3, GCS, Azure, PostgreSQL, MySQL, MongoDB, DynamoDB, and message queues (Kafka, SQS — peek without consuming).
- **LLM-Native**: Built-in MCP server for Claude Code, Codex, opencode, Gemini. One `dataql setup` wires them all up. Auto-activating Skills for Claude Code.
- **Self-Contained & Offline**: A single binary with the DuckDB engine and every driver embedded. No runtime downloads, no external services — works fully offline. Also shipped as a complete Docker image.
- **Familiar Syntax**: If you know SQL, you know DataQL. Type inference, parameterized queries, joins across sources, and rich export formats included.

## Quick Start

```bash
# Install DataQL (also: `go install github.com/adrianolaselva/dataql@latest`,
# or `docker run --rm -v "$PWD":/data ghcr.io/adrianolaselva/dataql ...`)
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash

# (Optional) wire DataQL into your AI agents (Claude Code, Codex, opencode).
# The install script already runs this for you.
dataql setup

# Query a CSV file
dataql run -f data.csv -q "SELECT * FROM data WHERE amount > 100"

# Query JSON from a URL
dataql run -f "https://api.example.com/data.json" -q "SELECT name, value FROM data"

# Query data from S3
dataql run -f "s3://bucket/data.parquet" -q "SELECT * FROM data LIMIT 10"

# Export results to JSON
dataql run -f data.csv -q "SELECT * FROM data" -e output.json -t json

# Interactive REPL mode
dataql run -f data.csv
# dataql> SELECT COUNT(*) FROM data;
# dataql> .tables
# dataql> .exit
```

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
- Message Queues — Kafka, Amazon SQS: peek (one-shot) **or stream continuously** with `--follow` (emit + windowed SQL). RabbitMQ & Pulsar are on the roadmap.

**Database Connectors:**
- PostgreSQL
- MySQL
- DuckDB
- MongoDB
- DynamoDB

**Key Capabilities:**
- Execute SQL queries using DuckDB syntax (OLAP-optimized)
- Export to CSV, JSONL, JSON, Excel, Parquet, XML, YAML, **Markdown & HTML** tables
- Transparent decompression of `.gz`, `.bz2`, `.xz` files
- **Parameterized queries** (`-p name=value`) for safe, reusable scripts
- **`describe`** command for instant exploratory statistics (min/max/mean/nulls/unique)
- **Data caching** to speed up repeated queries over the same source
- Interactive REPL mode with command history (`.tables`, `.schema`, `.exit`)
- Progress bar, parallel multi-file processing, automatic nested-JSON flattening
- Join data from multiple sources in a single query

**LLM Integration:**
- MCP Server for Claude Code, Codex, opencode, Gemini — one `dataql setup` configures them all
- Auto-activating Claude Code Skills
- Token-efficient data processing for AI assistants

### Commands at a glance

| Command | What it does |
|---------|--------------|
| `dataql run` | Query any file/source with SQL (or open the REPL) |
| `dataql describe` | Exploratory statistics for a dataset's columns |
| `dataql cache` | Inspect and manage the local query cache |
| `dataql mcp serve` | Run the MCP server (stdio) for AI agents |
| `dataql setup` | Auto-configure the MCP server for Claude Code, Codex, opencode |
| `dataql skills` | Install/list the DataQL skills for Claude Code |

Run `dataql <command> --help` for full options.

## LLM Integration

DataQL is designed for efficient use with Large Language Models, enabling AI assistants to query large datasets without loading entire files into context.

```bash
# One command wires up every agent on your machine (Claude Code, Codex, opencode):
# registers the MCP server, idempotent, no network, no manual config editing.
dataql setup

# Install the auto-activating Skills for Claude Code
dataql skills install

# Or run the MCP server directly (stdio) for any MCP-capable client
dataql mcp serve
```

> The install script runs `dataql setup` for you, so agents work right after install.

**Why use DataQL with LLMs?**

| Traditional Approach | With DataQL |
|---------------------|-------------|
| Send 10MB CSV to context | Run SQL query |
| ~100,000+ tokens | ~500 tokens |
| Limited by context window | No file size limit |

See [LLM Integration Guide](docs/llm-integration.md) for complete documentation.

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

### Run with Docker

DataQL ships as a self-contained image (DuckDB embedded, runs offline). Mount a
directory and query it — no install needed:

```bash
docker build -t dataql .   # or: make docker-build
docker run --rm -v "$PWD":/data dataql run -f /data/sales.csv -q "SELECT region, SUM(revenue) FROM sales GROUP BY region"
```

> A published image (`ghcr.io/adrianolaselva/dataql`) is added by the
> distribution milestone; until then, build locally as above.

### Install Options

**Specific version:**

Linux / macOS:
```bash
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --version v1.0.0
```

Windows (PowerShell):
```powershell
$env:DATAQL_VERSION="v1.0.0"; irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
```

**User installation (no sudo/admin required):**

Linux / macOS:
```bash
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --local
```

Windows (PowerShell):
```powershell
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

### Update

**Upgrade to latest version:**

Linux / macOS:
```bash
# Only upgrades if a newer version is available
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --upgrade

# Force reinstall (same or different version)
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --force

# Clean install (remove all versions first, then install)
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --clean --force
```

Windows (PowerShell):
```powershell
# Force reinstall
$env:DATAQL_FORCE="true"; irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
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
| DynamoDB | `dynamodb://` | `-f "dynamodb://region/table-name"` |

### Command Line Options

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--file` | `-f` | Input file, URL, or database connection | Required |
| `--delimiter` | `-d` | CSV delimiter (only for CSV files) | `,` |
| `--query` | `-q` | SQL query to execute | - |
| `--export` | `-e` | Export path | - |
| `--type` | `-t` | Export format (`csv`, `jsonl`, `json`, `excel`, `parquet`, `xml`, `yaml`) | - |
| `--storage` | `-s` | DuckDB file path (for persistence) | In-memory |
| `--lines` | `-l` | Limit number of lines/records to read | All |
| `--collection` | `-c` | Custom table name | Filename |

### Examples

**Stream a queue/topic (`--follow`):**

```bash
# Emit each message as JSON as it arrives (tail -f for Kafka/SQS)
dataql run -f kafka://broker:9092/events --follow --max-messages 100

# Rolling SQL over a window: re-run the query every 2s over the last 1000 messages
dataql run -f kafka://broker:9092/logs --follow \
  --window 1000 --interval 2s \
  -q "SELECT level, COUNT(*) AS n FROM stream GROUP BY level ORDER BY n DESC"

# A time window + idle/duration stops (great for sampling and scripts)
dataql run -f "sqs://my-queue?region=us-east-1" --follow \
  --window 30s --idle-timeout 10s --duration 5m \
  -q "SELECT COUNT(*) AS events_30s FROM stream"
```

> `--follow` consumes (advances the offset / deletes messages). The window table
> is named `stream`. Stop with `Ctrl-C`, `--max-messages`, `--duration`, or
> `--idle-timeout`.

**Exploratory statistics (`describe`):**

```bash
# Instant column profile: type, nulls, unique, min/max/mean/std
dataql describe -f sales.csv
```

**Parameterized queries (safe & reusable):**

```bash
dataql run -f orders.csv \
  -q "SELECT * FROM orders WHERE region = :region AND amount > :min" \
  -p region=North -p min=100
```

**Compressed files (transparent):**

```bash
# .gz, .bz2 and .xz are decompressed on the fly
dataql run -f logs.jsonl.gz -q "SELECT level, COUNT(*) FROM logs GROUP BY level"
```

**Cache a source for repeated queries:**

```bash
dataql run -f huge.parquet -q "SELECT ..." --cache   # first run loads & caches
dataql cache list                                     # inspect cached datasets
```

**Run via Docker (no install):**

```bash
docker run --rm -v "$PWD":/data ghcr.io/adrianolaselva/dataql \
  run -f /data/sales.csv -q "SELECT region, SUM(revenue) FROM sales GROUP BY region"
```

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

**Persist to DuckDB File:**

```bash
dataql run -f data.csv -d "," -s ./database.duckdb
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

**Peek at SQS messages (without consuming):**

```bash
dataql run -f "sqs://my-events-queue?region=us-east-1" \
  -q "SELECT message_id, body_event_type, timestamp FROM my_events_queue"
```

**Read from stdin:**

```bash
cat data.csv | dataql run -f - -q "SELECT * FROM stdin_data WHERE value > 100"
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

DataQL uses DuckDB under the hood, supporting standard SQL syntax optimized for analytical queries:

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
- [Architecture](docs/architecture.md) - System architecture and design diagrams
- [CLI Reference](docs/cli-reference.md) - Complete command-line reference
- [Data Sources](docs/data-sources.md) - Working with S3, GCS, Azure, URLs, and stdin
- [Database Connections](docs/databases.md) - Connect to PostgreSQL, MySQL, DuckDB, MongoDB
- [LLM Integration](docs/llm-integration.md) - Use DataQL with Claude, Codex, Gemini
- [MCP Setup](docs/mcp-setup.md) - Configure MCP server for LLM integration
- [Examples](docs/examples.md) - Real-world usage examples and automation scripts

## Development

### Prerequisites

- Go 1.26 or higher
- GCC (for CGO compilation - required for DuckDB)
- Docker and Docker Compose (for E2E tests)

### Building

```bash
make build
```

### Testing

```bash
make test             # unit tests
make coverage         # unit tests with an HTML coverage report
make coverage-check   # enforce the coverage ratchet (what CI runs)
make e2e              # full E2E suite: provision services, run, tear down (Docker)
```

### Quality gates

Every PR runs gates designed so quality only ever ratchets up — see
[CONTRIBUTING.md](CONTRIBUTING.md) for the full table:

| Gate | Enforces |
|------|----------|
| **Coverage ratchet** | total coverage never drops below `.coverage-baseline` (live % in the Codecov badge above) |
| **Lint** | `golangci-lint` (govet, staticcheck, errcheck, …) — blocking |
| **Vulnerability scan** | `govulncheck` — no known vulnerabilities in called code |
| **Security scan** | `gosec` — blocking |
| **Offline self-sufficiency** | the binary runs with the network disabled (no runtime downloads) |
| **Self-contained release build** | the release binary embeds DuckDB and passes the offline smoke |
| **Docker image** | the image builds and runs a query offline |
| **E2E** | full suite against emulated backends (advisory) |

### E2E Test Coverage

DataQL includes comprehensive E2E tests for all data sources:

| Data Source | Tests | Status |
|-------------|-------|--------|
| PostgreSQL | 26 | SELECT, WHERE, ORDER BY, LIMIT, aggregates, exports |
| MySQL | 26 | SELECT, WHERE, ORDER BY, LIMIT, aggregates, exports |
| MongoDB | 20+ | Collections, queries, filters, exports |
| Kafka | 10+ | Peek mode, message parsing, exports |
| S3 (LocalStack) | 13 | CSV, JSON, JSONL file reading, queries, exports |
| SQS (LocalStack) | 16 | Message reading, filtering, aggregation, exports |
| DynamoDB (LocalStack) | ✓ | Scan, queries, filters, exports |
| URL (HTTP) | 3 | CSV/JSON over HTTP, WHERE filtering |

See [e2e/COVERAGE.md](e2e/COVERAGE.md) for the full source/feature coverage matrix and [e2e/README.md](e2e/README.md) for E2E testing docs.

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

## About This Project

This is a rewrite of [csvql](https://github.com/adrianolaselva/csvql), an earlier experiment I did back in 2019. The original was simple and limited. This version? Built entirely with AI assistance (Claude Code). I wanted to see how far AI-assisted development could go, and honestly, it went pretty far. The code, docs, tests - all of it came from conversations with an AI. Make of that what you will.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [csvql](https://github.com/adrianolaselva/csvql) - The original project that inspired this rewrite
- [Claude Code](https://claude.ai/claude-code) - AI assistant that helped build this entire project
- [DuckDB](https://duckdb.org/) - Embedded analytical database engine
- [Cobra](https://github.com/spf13/cobra) - CLI framework
- [go-duckdb](https://github.com/marcboeker/go-duckdb) - DuckDB driver for Go
