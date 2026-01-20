<p align="center">
  <img src="img/dataql.png" alt="DataQL Logo" width="350">
</p>

<h1 align="center">DataQL Documentation</h1>

<p align="center">
  <strong>Query any data file using SQL. One command, instant results.</strong>
</p>

---

## Documentation Index

| Document | Description |
|----------|-------------|
| [Getting Started](getting-started.md) | Installation and Hello World examples |
| [CLI Reference](cli-reference.md) | Complete command-line reference |
| [Data Sources](data-sources.md) | Working with S3, GCS, Azure, URLs, and stdin |
| [Database Connections](databases.md) | Connect to PostgreSQL, MySQL, DuckDB, MongoDB |
| [LLM Integration](llm-integration.md) | Use DataQL with Claude, Codex, Gemini |
| [MCP Setup](mcp-setup.md) | Configure MCP server for LLM integration |
| [Examples](examples.md) | Real-world usage examples and automation scripts |

## Quick Links

- [GitHub Repository](https://github.com/adrianolaselva/dataql)
- [Report Issues](https://github.com/adrianolaselva/dataql/issues)
- [Releases](https://github.com/adrianolaselva/dataql/releases)

## Quick Example

```bash
# Install DataQL
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash

# Query a CSV file
dataql run -f sales.csv -q "SELECT product, SUM(amount) FROM sales GROUP BY product"

# Query JSON from S3
dataql run -f "s3://bucket/data.json" -q "SELECT * FROM data WHERE status = 'active'"

# Export to Parquet
dataql run -f data.csv -q "SELECT * FROM data" -e output.parquet -t parquet
```

## Supported Formats

| Format | Extensions | Read | Export |
|--------|------------|------|--------|
| CSV | `.csv` | Yes | Yes |
| JSON | `.json` | Yes | Yes |
| JSONL | `.jsonl`, `.ndjson` | Yes | Yes |
| Parquet | `.parquet` | Yes | Yes |
| Excel | `.xlsx`, `.xls` | Yes | Yes |
| XML | `.xml` | Yes | Yes |
| YAML | `.yaml`, `.yml` | Yes | Yes |
| Avro | `.avro` | Yes | No |
| ORC | `.orc` | Yes | No |

## Supported Data Sources

| Source | Protocol | Example |
|--------|----------|---------|
| Local files | Path | `-f data.csv` |
| HTTP/HTTPS | URL | `-f "https://example.com/data.csv"` |
| Amazon S3 | `s3://` | `-f "s3://bucket/data.csv"` |
| Google Cloud Storage | `gs://` | `-f "gs://bucket/data.json"` |
| Azure Blob | `az://` | `-f "az://container/data.parquet"` |
| PostgreSQL | `postgres://` | `-f "postgres://user:pass@host/db?table=t"` |
| MySQL | `mysql://` | `-f "mysql://user:pass@host/db?table=t"` |
| MongoDB | `mongodb://` | `-f "mongodb://host/db?collection=c"` |
| DynamoDB | `dynamodb://` | `-f "dynamodb://region/table"` |
| Standard input | `-` | `cat data.csv \| dataql run -f -` |
