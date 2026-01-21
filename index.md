---
layout: default
title: DataQL - SQL for Any Data Format
---

# DataQL

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://go.dev)
[![Build](https://github.com/adrianolaselva/dataql/actions/workflows/build.yml/badge.svg)](https://github.com/adrianolaselva/dataql/actions/workflows/build.yml)
[![CI](https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml/badge.svg)](https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/adrianolaselva/dataql)](https://goreportcard.com/report/github.com/adrianolaselva/dataql)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A powerful CLI tool for querying and transforming data across multiple formats

DataQL is a CLI tool developed in Go that allows you to query and manipulate data files using SQL statements.
It loads data into a DuckDB database (in-memory or file-based) with automatic type inference, enabling powerful SQL operations optimized for analytical queries.

---

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
- **Universal Format Support**: CSV, JSON, Parquet, Excel, XML, YAML, Avro, ORC - all queryable with SQL.
- **Any Data Source**: Local files, URLs, S3, GCS, Azure, PostgreSQL, MySQL, MongoDB.
- **LLM-Native**: Built-in MCP server for Claude, Codex, Gemini. Skills for Claude Code.
- **Zero Setup**: Single binary, no dependencies, no configuration files.
- **Familiar Syntax**: If you know SQL, you know DataQL.

---

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
- Execute SQL queries using DuckDB syntax (OLAP-optimized)
- Export results to CSV, JSONL, JSON, Excel, Parquet, XML, YAML formats
- Interactive REPL mode with command history
- Progress bar for large file operations
- Parallel file processing for multiple inputs
- Automatic flattening of nested JSON objects
- Join data from multiple sources

**LLM Integration:**
- MCP Server for Claude Code, OpenAI Codex, Google Gemini
- Auto-activating Claude Code Skills
- Token-efficient data processing for AI assistants

## Quick Start

### Installation

**Linux / macOS:**

```bash
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
```

### Hello World

```bash
# Create a sample CSV file
echo -e "id,name,age\n1,Alice,28\n2,Bob,35\n3,Charlie,42" > users.csv

# Query the data
dataql run -f users.csv -q "SELECT * FROM users WHERE age > 30"
```

### Basic Usage

```bash
# Query a CSV file
dataql run -f data.csv -q "SELECT * FROM data WHERE amount > 100"

# Query a JSON file
dataql run -f users.json -q "SELECT name, email FROM users WHERE status = 'active'"

# Query from URL
dataql run -f "https://example.com/data.csv" -q "SELECT * FROM data"

# Query from S3
dataql run -f "s3://my-bucket/data.csv" -q "SELECT * FROM data"

# Query from PostgreSQL
dataql run -f "postgres://user:pass@localhost/db?table=users" -q "SELECT * FROM users"

# Read from stdin
cat data.csv | dataql run -f - -q "SELECT * FROM stdin"

# Export results
dataql run -f input.csv -q "SELECT * FROM input" -e output.jsonl -t jsonl
```

### Interactive Mode

```bash
dataql run -f sales.csv
```

```
dataql> .tables
dataql> .schema sales
dataql> SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC;
dataql> .exit
```

## Documentation

- [Getting Started](docs/getting-started.md) - Installation and Hello World examples
- [CLI Reference](docs/cli-reference.md) - Complete command-line reference
- [Data Sources](docs/data-sources.md) - Working with S3, GCS, Azure, URLs, and stdin
- [Database Connections](docs/databases.md) - Connect to PostgreSQL, MySQL, DuckDB, MongoDB
- [LLM Integration](docs/llm-integration.md) - Use DataQL with Claude, Codex, Gemini
- [MCP Setup](docs/mcp-setup.md) - Configure MCP server for LLM integration
- [Examples](docs/examples.md) - Real-world usage examples and automation scripts

## About

A rewrite of [csvql](https://github.com/adrianolaselva/csvql) (2019), built entirely with AI assistance. An experiment in AI-assisted development that turned out pretty well.

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/adrianolaselva/dataql/blob/main/LICENSE) file for details.
