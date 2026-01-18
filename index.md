---
layout: default
title: DataQL - SQL for Any Data Format
---

# DataQL

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)](https://go.dev)
[![Build](https://github.com/adrianolaselva/dataql/actions/workflows/build.yml/badge.svg)](https://github.com/adrianolaselva/dataql/actions/workflows/build.yml)
[![CI](https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml/badge.svg)](https://github.com/adrianolaselva/dataql/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/adrianolaselva/dataql)](https://goreportcard.com/report/github.com/adrianolaselva/dataql)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A powerful CLI tool for querying and transforming data across multiple formats

DataQL is a CLI tool developed in Go that allows you to query and manipulate data files using SQL statements.
It loads data into an SQLite database (in-memory or file-based) enabling powerful SQL operations on your data.

## Features

**Supported Input Formats:**
- CSV (with configurable delimiter)
- JSON (arrays or single objects)
- JSONL/NDJSON (newline-delimited JSON)
- XML
- YAML
- Parquet

**Data Sources:**
- Local files
- HTTP/HTTPS URLs
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- PostgreSQL databases
- MySQL databases
- DuckDB databases

**Key Capabilities:**
- Execute SQL queries using SQLite syntax
- Export results to CSV or JSONL formats
- Interactive REPL mode with command history
- Progress bar for large file operations
- Parallel file processing for multiple inputs
- Automatic flattening of nested JSON objects

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

### Basic Usage

```bash
# Query a CSV file
dataql run -f data.csv -q "SELECT * FROM data WHERE amount > 100"

# Query a JSON file
dataql run -f users.json -q "SELECT name, email FROM users WHERE status = 'active'"

# Query from URL
dataql run -f "https://example.com/data.csv" -q "SELECT * FROM data"

# Export results
dataql run -f input.csv -q "SELECT * FROM input" -e output.jsonl -t jsonl
```

### Interactive Mode

```bash
dataql run -f sales.csv
```

```
dataql> SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC;
```

## Documentation

- [Installation Guide](https://github.com/adrianolaselva/dataql#installation)
- [Usage Examples](https://github.com/adrianolaselva/dataql#usage)
- [SQL Reference](https://github.com/adrianolaselva/dataql#sql-reference)
- [Contributing](https://github.com/adrianolaselva/dataql#contributing)

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/adrianolaselva/dataql/blob/main/LICENSE) file for details.
