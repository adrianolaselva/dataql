<p align="center">
  <img src="img/dataql.jpg" alt="DataQL Logo" width="250">
</p>

# LLM Integration Guide

DataQL enables Large Language Models (LLMs) to efficiently query and transform data without loading entire files into context, dramatically reducing token consumption.

---

## The Problem

LLMs are powerful, but they have a fundamental limitation: **context windows**. When you need to analyze a data file, you face tough choices:

1. **Send the entire file**: A 10MB CSV consumes ~100,000+ tokens. At $3 per million tokens, that's $0.30+ per query. And many files simply won't fit.

2. **Sample the data**: You might miss important patterns. "Show me the first 100 rows" rarely answers real questions.

3. **Pre-process externally**: Break the conversational flow. Write scripts. Lose the interactive nature of LLM assistance.

## The Solution

DataQL acts as a **data access layer** for LLMs. Instead of sending raw data, the LLM sends SQL queries and receives only the results it needs:

```
┌─────────────────┐                      ┌─────────────────┐
│      LLM        │  "SELECT region,     │    DataQL       │
│                 │   SUM(sales)         │                 │
│ "Analyze sales  │   FROM sales         │  ┌───────────┐  │
│  by region"     │ ──────────────────►  │  │ 10GB CSV  │  │
│                 │                      │  └───────────┘  │
│                 │  ◄──────────────────  │                 │
│                 │  {North: $1.2M,      │  Query Result:  │
│                 │   South: $980K}      │  2 rows, 50     │
│                 │                      │  tokens         │
└─────────────────┘                      └─────────────────┘
```

## Why This Matters

| Scenario | Without DataQL | With DataQL |
|----------|---------------|-------------|
| Analyze 10MB CSV | ~100,000 tokens ($3+) | ~500 tokens ($0.01) |
| Find specific records | Load everything, filter in context | SQL WHERE clause |
| Aggregate statistics | Manual calculation by LLM | SQL SUM, AVG, COUNT |
| Join multiple files | Impossible in context | Single SQL JOIN |
| Query cloud storage | Download → Script → Parse | One command |
| Repeated analysis | Re-send file each time | Query as needed |

### Real Numbers

- **10MB CSV file**: ~50,000 rows, ~100,000 tokens
- **Simple query result**: 10 rows, ~200 tokens
- **Token reduction**: **99.8%**
- **Cost reduction**: From $0.30 to $0.0006 per query

---

## Quick Start

### 1. Install DataQL Skills (Recommended for Claude Code)

```bash
dataql skills install
```

This installs skills that teach Claude Code how to use DataQL automatically.

### 2. Use in Claude Code

Just describe what you need in natural language:

- "Query users.csv and show users older than 30"
- "Join orders.json with customers.csv by customer_id"
- "Get total revenue from sales.parquet grouped by region"
- "Count how many records are in this CSV file"

### 3. Or Use MCP Server (For Any LLM)

```bash
dataql mcp serve
```

Configure your LLM client to connect to the MCP server (see [MCP Setup Guide](mcp-setup.md)).

---

## Integration Methods

### Method 1: MCP Server (Best for Automation)

The Model Context Protocol (MCP) provides native integration with LLMs:

**Supported Clients:**
- Claude Code (Anthropic)
- OpenAI Codex
- Google Gemini CLI
- Any MCP-compatible client

**Configuration:** See [MCP Setup Guide](mcp-setup.md)

### Method 2: Skills (Best for Interactive Use in Claude Code)

Skills are markdown files that teach Claude how to use DataQL:

```bash
# Install to global directory (all projects)
dataql skills install --global

# Install to current project only
dataql skills install --project

# List installed skills
dataql skills list
```

### Method 3: Direct CLI (Universal - Any LLM with Bash)

Any LLM with shell access can use DataQL directly:

```bash
# Query a file
dataql run -f data.csv -q "SELECT * FROM data WHERE status='active'"

# Get schema
dataql run -f data.csv -q ".schema data"

# Join multiple files
dataql run -f users.csv -f orders.json -q "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
```

---

## Token Efficiency Patterns

### Pattern 1: Schema-First Approach

Always understand the data structure before running complex queries:

```bash
# Step 1: Get schema (minimal tokens)
dataql run -f data.csv -q ".schema data"

# Step 2: Query specific columns
dataql run -f data.csv -q "SELECT name, email FROM data WHERE status='active'"
```

### Pattern 2: Aggregation Pipeline

Instead of returning all rows, use aggregations:

```bash
# Bad: Returns potentially thousands of rows
dataql run -f sales.csv -q "SELECT * FROM sales"

# Good: Returns summary statistics
dataql run -f sales.csv -q "SELECT category, COUNT(*) as count, SUM(amount) as total FROM sales GROUP BY category"
```

### Pattern 3: Incremental Refinement

Start broad and narrow down:

```bash
# Step 1: Understand the data
dataql run -f logs.json -q "SELECT DISTINCT level FROM logs"

# Step 2: Count by category
dataql run -f logs.json -q "SELECT level, COUNT(*) FROM logs GROUP BY level"

# Step 3: Get specific records
dataql run -f logs.json -q "SELECT * FROM logs WHERE level='error' LIMIT 20"
```

### Pattern 4: Sampling Large Datasets

Preview data before processing:

```bash
# Preview first 5 rows
dataql run -f large_dataset.parquet -q "SELECT * FROM large_dataset LIMIT 5"

# Get total count
dataql run -f large_dataset.parquet -q "SELECT COUNT(*) FROM large_dataset"
```

---

## MCP Tools Reference

When using the MCP server, these tools are available:

### dataql_query

Execute SQL queries on any data source.

**Parameters:**
- `source` (required): File path, URL, S3 URI, or database connection string
- `query` (required): SQL query to execute
- `delimiter` (optional): CSV delimiter character

**Example:**
```json
{
  "name": "dataql_query",
  "arguments": {
    "source": "users.csv",
    "query": "SELECT * FROM users WHERE age > 30 LIMIT 10"
  }
}
```

### dataql_schema

Get the structure of a data source.

**Parameters:**
- `source` (required): File path, URL, or database connection string

**Example:**
```json
{
  "name": "dataql_schema",
  "arguments": {
    "source": "sales.json"
  }
}
```

### dataql_preview

Preview first N rows of data.

**Parameters:**
- `source` (required): Data source
- `limit` (optional): Number of rows (default: 5, max: 100)

**Example:**
```json
{
  "name": "dataql_preview",
  "arguments": {
    "source": "data.parquet",
    "limit": 10
  }
}
```

### dataql_aggregate

Perform aggregation operations.

**Parameters:**
- `source` (required): Data source
- `column` (required): Column to aggregate
- `operation` (required): count, sum, avg, min, max
- `group_by` (optional): Column to group by

**Example:**
```json
{
  "name": "dataql_aggregate",
  "arguments": {
    "source": "sales.csv",
    "column": "amount",
    "operation": "sum",
    "group_by": "category"
  }
}
```

---

## Platform-Specific Guides

### Claude Code

1. **Install Skills:**
   ```bash
   dataql skills install --global
   ```

2. **Use Naturally:**
   Just ask Claude to analyze your data files. The skills teach Claude when and how to use DataQL.

3. **Or Configure MCP:**
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

### OpenAI Codex

Add to `~/.codex/config.toml`:

```toml
[mcp.servers.dataql]
transport = "stdio"
command = "dataql"
args = ["mcp", "serve"]
```

### Google Gemini CLI

Add to your Gemini CLI configuration:

```yaml
mcpServers:
  dataql:
    command: dataql
    args: ["mcp", "serve"]
```

### Other LLMs

For LLMs with shell/bash access, use direct CLI commands:

```bash
# Set up an alias for easier use
alias dq='dataql run'

# Query files
dq -f data.csv -q "SELECT * FROM data WHERE amount > 100"
```

---

## Real-World Examples

### Example 1: Analyze Sales Data

```bash
# 1. Understand the data
dataql run -f sales.csv -q ".schema sales"

# 2. Get overview
dataql run -f sales.csv -q "SELECT COUNT(*) as total_transactions, SUM(amount) as revenue FROM sales"

# 3. Top products
dataql run -f sales.csv -q "SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC LIMIT 10"

# 4. Sales by region
dataql run -f sales.csv -q "SELECT region, COUNT(*) as orders, AVG(amount) as avg_order FROM sales GROUP BY region"
```

### Example 2: Join Customer and Order Data

```bash
dataql run -f customers.csv -f orders.json -q "
SELECT
  c.name,
  c.email,
  COUNT(o.id) as order_count,
  SUM(o.total) as total_spent
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email
ORDER BY total_spent DESC
LIMIT 20"
```

### Example 3: Filter and Export

```bash
# Find active users and export
dataql run -f users.json -q "SELECT id, name, email FROM users WHERE status = 'active'" -e active_users.csv -t csv

# Export as JSONL for processing
dataql run -f data.parquet -q "SELECT * FROM data WHERE region = 'US'" -e us_data.jsonl -t jsonl
```

### Example 4: Query Remote Data

```bash
# Query from URL
dataql run -f "https://api.example.com/data.json" -q "SELECT * FROM data LIMIT 10"

# Query from S3
dataql run -f "s3://my-bucket/analytics/sales.parquet" -q "SELECT * FROM sales WHERE year = 2024"

# Query from database
dataql run -f "postgres://user:pass@localhost/db?table=users" -q "SELECT * FROM users WHERE active = true"
```

---

## Best Practices

1. **Always use LIMIT** for exploratory queries
2. **Select specific columns** instead of `SELECT *`
3. **Use aggregations** when you need summaries
4. **Check schema first** before complex queries
5. **Export large results** to files instead of context
6. **Use JOINs** to combine data from multiple sources
7. **Filter early** with WHERE clauses to reduce data

---

## Troubleshooting

### Skills Not Activating in Claude Code

1. Check installation: `dataql skills list`
2. Restart Claude Code
3. Run with debug: `claude --debug`

### MCP Server Not Connecting

1. Verify DataQL is installed: `dataql --version`
2. Test MCP server: `echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | dataql mcp serve`
3. Check client configuration

### Query Errors

1. Check schema: `dataql run -f file.csv -q ".schema tablename"`
2. Verify column names match exactly
3. For CSV, check delimiter: `dataql run -f file.csv -d ";" -q "..."`

---

## See Also

- [MCP Setup Guide](mcp-setup.md) - Detailed MCP configuration
- [CLI Reference](cli-reference.md) - Complete command reference
- [Data Sources](data-sources.md) - Working with S3, URLs, databases
- [Examples](examples.md) - More usage examples
