# MCP Server Setup Guide

Complete guide for configuring DataQL as a Model Context Protocol (MCP) server for LLM integration.

## Overview

The MCP server enables LLMs to query data files using SQL without loading entire files into context. This dramatically reduces token consumption while providing powerful data analysis capabilities.

## Prerequisites

- DataQL installed and available in PATH
- LLM client with MCP support (Claude Code, OpenAI Codex, Gemini CLI, etc.)

Verify installation:
```bash
dataql --version
```

## Quick Setup

### Claude Code

Add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "dataql": {
      "type": "stdio",
      "command": "dataql",
      "args": ["mcp", "serve"]
    }
  }
}
```

Restart Claude Code to load the new configuration.

### OpenAI Codex

Add to `~/.codex/config.toml`:

```toml
[mcp.servers.dataql]
transport = "stdio"
command = "dataql"
args = ["mcp", "serve"]
```

### Google Gemini CLI

Add to your Gemini CLI configuration file:

```yaml
mcpServers:
  dataql:
    command: dataql
    args: ["mcp", "serve"]
```

## Available MCP Tools

Once configured, the following tools become available to your LLM:

| Tool | Description |
|------|-------------|
| `dataql_query` | Execute SQL queries on data sources |
| `dataql_schema` | Get structure/schema of a data source |
| `dataql_preview` | Preview first N rows |
| `dataql_aggregate` | Perform count, sum, avg, min, max operations |

## Testing Your Setup

### 1. Verify MCP Server Starts

```bash
echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | dataql mcp serve
```

You should see a JSON response listing available tools.

### 2. Test a Query

```bash
# Create test data
echo -e "id,name,age\n1,Alice,28\n2,Bob,35\n3,Charlie,42" > test.csv

# Test via MCP
echo '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"dataql_query","arguments":{"source":"test.csv","query":"SELECT * FROM test WHERE age > 30"}},"id":2}' | dataql mcp serve
```

### 3. Test in Your LLM Client

After configuring, ask your LLM:

> "Use dataql to show me the schema of test.csv"

The LLM should use the `dataql_schema` tool to get the file structure.

## Tool Reference

### dataql_query

Execute SQL queries on any data source.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| source | Yes | File path, URL, S3 URI, or database connection |
| query | Yes | SQL query to execute |
| delimiter | No | CSV delimiter (default: comma) |

**Example Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "dataql_query",
    "arguments": {
      "source": "sales.csv",
      "query": "SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC LIMIT 10"
    }
  },
  "id": 1
}
```

### dataql_schema

Get the structure of a data source.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| source | Yes | File path, URL, or database connection |

**Example Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "dataql_schema",
    "arguments": {
      "source": "users.json"
    }
  },
  "id": 1
}
```

### dataql_preview

Preview first N rows of a data source.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| source | Yes | Data source |
| limit | No | Number of rows (default: 5, max: 100) |

**Example Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "dataql_preview",
    "arguments": {
      "source": "data.parquet",
      "limit": 10
    }
  },
  "id": 1
}
```

### dataql_aggregate

Perform aggregation operations on a column.

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| source | Yes | Data source |
| column | Yes | Column to aggregate |
| operation | Yes | count, sum, avg, min, max |
| group_by | No | Column to group results by |

**Example Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "dataql_aggregate",
    "arguments": {
      "source": "sales.csv",
      "column": "amount",
      "operation": "sum",
      "group_by": "category"
    }
  },
  "id": 1
}
```

## Advanced Configuration

### Debug Mode

Start the server with debug logging:

```bash
dataql mcp serve --debug
```

### Environment Variables

The MCP server inherits environment variables, useful for cloud storage credentials:

```bash
# AWS S3
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Then query S3 files
# dataql_query with source="s3://bucket/file.csv"
```

### Working Directory

The MCP server runs in the current working directory. File paths are relative to where the server starts.

For Claude Code, this is typically your project directory.

## Supported Data Sources

The MCP server supports all DataQL data sources:

### Local Files
- CSV, JSON, JSONL, XML, YAML
- Parquet, Avro, ORC
- Excel (.xlsx, .xls)

### Remote Sources
- HTTP/HTTPS URLs
- Amazon S3 (`s3://bucket/path/file.csv`)
- Google Cloud Storage (`gs://bucket/path/file.csv`)
- Azure Blob Storage (`azure://container/path/file.csv`)

### Databases
- PostgreSQL (`postgres://user:pass@host/db?table=name`)
- MySQL (`mysql://user:pass@host/db?table=name`)
- MongoDB (`mongodb://user:pass@host/db?collection=name`)

## Security Considerations

1. **File Access**: The MCP server can access any file the user running it can access. Be mindful of sensitive data.

2. **Database Credentials**: Use environment variables for database passwords instead of connection strings.

3. **Remote Sources**: Ensure proper authentication is configured for cloud storage.

4. **Sandboxing**: Consider running in a container or restricted environment for untrusted use cases.

## Troubleshooting

### Server Won't Start

**Error:** Command not found
```bash
# Ensure dataql is in PATH
which dataql

# Or use full path in configuration
"command": "/usr/local/bin/dataql"
```

### Connection Refused

**Error:** LLM client can't connect

1. Verify the server starts manually:
   ```bash
   echo '{}' | dataql mcp serve
   ```

2. Check client configuration syntax

3. Restart the LLM client

### Query Errors

**Error:** Table not found

The table name is derived from the filename without extension:
- `users.csv` → table name is `users`
- `my-data.json` → table name is `my-data`

Use `.schema` to verify:
```json
{"name": "dataql_schema", "arguments": {"source": "file.csv"}}
```

### Performance Issues

For large files, use:
- LIMIT clauses in queries
- Aggregations instead of returning all rows
- Specific column selection instead of SELECT *

## See Also

- [LLM Integration Guide](llm-integration.md) - Overview of all integration methods
- [CLI Reference](cli-reference.md) - DataQL command-line options
- [Data Sources](data-sources.md) - Detailed data source documentation
