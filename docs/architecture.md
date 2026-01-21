<p align="center">
  <img src="img/dataql.png" alt="DataQL Logo" width="200">
</p>

# DataQL Architecture

This document provides a comprehensive overview of DataQL's architecture, including system components, data flow, and key operations.

## Table of Contents

- [System Overview](#system-overview)
- [Data Flow](#data-flow)
- [Component Structure](#component-structure)
- [Key Operations](#key-operations)
  - [Query Execution](#query-execution)
  - [Interactive REPL Mode](#interactive-repl-mode)
  - [MCP Server for LLMs](#mcp-server-for-llms)
- [Design Patterns](#design-patterns)
- [Package Reference](#package-reference)

---

## System Overview

DataQL is a CLI tool that enables SQL queries on any data file. The architecture follows a layered design with clear separation of concerns:

```mermaid
flowchart TB
    subgraph CLI["CLI Layer"]
        RUN["dataql run"]
        MCP["dataql mcp serve"]
        SKILLS["dataql skills"]
    end

    subgraph CORE["Core Engine"]
        DATAQL["DataQL Engine<br/><code>internal/dataql</code>"]
    end

    subgraph INPUT["Input Resolution"]
        STDIN["Stdin Handler"]
        URL["URL Handler"]
        S3["S3 Handler"]
        GCS["GCS Handler"]
        AZURE["Azure Handler"]
    end

    subgraph HANDLERS["File Handlers"]
        CSV["CSV"]
        JSON["JSON/JSONL"]
        PARQUET["Parquet"]
        EXCEL["Excel"]
        XML["XML/YAML"]
        AVRO["Avro/ORC"]
    end

    subgraph CONNECTORS["Database Connectors"]
        PG["PostgreSQL"]
        MYSQL["MySQL"]
        MONGO["MongoDB"]
        DYNAMO["DynamoDB"]
        DUCK["DuckDB"]
    end

    subgraph MQ["Message Queues"]
        SQS["AWS SQS"]
        KAFKA["Apache Kafka"]
    end

    subgraph STORAGE["Storage Layer"]
        DUCKDB[("DuckDB<br/>(in-memory or file)")]
    end

    subgraph OUTPUT["Output"]
        TABLE["Table Display"]
        EXPORT["Export<br/>CSV/JSON/Parquet/..."]
        REPL["Interactive REPL"]
        MCPOUT["MCP JSON Response"]
    end

    CLI --> CORE
    CORE --> INPUT
    INPUT --> HANDLERS
    INPUT --> CONNECTORS
    INPUT --> MQ
    HANDLERS --> STORAGE
    CONNECTORS --> STORAGE
    MQ --> STORAGE
    STORAGE --> OUTPUT
```

### Architecture Highlights

| Layer | Description |
|-------|-------------|
| **CLI Layer** | Entry points for user interaction (run, mcp, skills commands) |
| **Core Engine** | Orchestrates data loading, query execution, and result formatting |
| **Input Resolution** | Resolves remote sources (S3, HTTP, etc.) to local files |
| **File Handlers** | Format-specific data loaders (CSV, JSON, Parquet, etc.) |
| **Database Connectors** | Direct connections to databases (PostgreSQL, MySQL, MongoDB, etc.) |
| **Message Queues** | Peek-mode readers for SQS, Kafka (non-consuming) |
| **Storage Layer** | DuckDB database for SQL query execution |
| **Output** | Result formatting and export functionality |

---

## Data Flow

The data processing pipeline transforms input from any source into queryable SQL tables:

```mermaid
flowchart LR
    subgraph INPUT["1. Input Sources"]
        direction TB
        A1["Local Files"]
        A2["HTTP/HTTPS URLs"]
        A3["S3/GCS/Azure"]
        A4["Databases"]
        A5["Message Queues"]
        A6["Stdin"]
    end

    subgraph RESOLVE["2. Resolution"]
        direction TB
        B1["Download to Temp"]
        B2["Format Detection"]
    end

    subgraph LOAD["3. Loading"]
        direction TB
        C1["FileHandler.Import()"]
        C2["Schema Extraction"]
        C3["Data Transformation"]
    end

    subgraph STORE["4. Storage"]
        direction TB
        D1["CREATE TABLE"]
        D2["INSERT Rows"]
        D3[("DuckDB DB")]
    end

    subgraph QUERY["5. Query"]
        direction TB
        E1["SQL Parsing"]
        E2["Query Execution"]
        E3["Result Set"]
    end

    subgraph OUTPUT["6. Output"]
        direction TB
        F1["Console Table"]
        F2["Export File"]
        F3["MCP JSON"]
    end

    INPUT --> RESOLVE --> LOAD --> STORE --> QUERY --> OUTPUT
```

### Pipeline Stages

1. **Input Sources**: DataQL accepts data from multiple sources:
   - Local files (CSV, JSON, Parquet, Excel, etc.)
   - Remote URLs (HTTP/HTTPS)
   - Cloud storage (S3, GCS, Azure Blob)
   - Databases (PostgreSQL, MySQL, MongoDB, DynamoDB, DuckDB)
   - Message queues (SQS, Kafka - peek mode)
   - Standard input (stdin)

2. **Resolution**: Remote sources are downloaded to temporary files. Format is detected from file extension or URL scheme.

3. **Loading**: The appropriate FileHandler parses the data:
   - Extracts schema (column names and types)
   - Transforms nested structures (JSON flattening)
   - Handles type conversions

4. **Storage**: Data is loaded into DuckDB:
   - Tables created dynamically from schema
   - Column types inferred automatically (BIGINT, DOUBLE, BOOLEAN, VARCHAR)
   - Supports multiple tables for JOINs

5. **Query**: SQL queries are executed against DuckDB:
   - Full DuckDB SQL syntax support (analytical/OLAP optimized)
   - JOINs across multiple data sources
   - Aggregations, filtering, sorting

6. **Output**: Results are formatted for the target:
   - Console table with colors
   - Export to various formats
   - JSON for MCP/LLM integration

---

## Component Structure

The codebase is organized into clear packages with defined responsibilities:

```mermaid
flowchart TB
    subgraph CMD["cmd/"]
        main["main.go<br/><small>Entry point</small>"]
        dataqlctl["dataqlctl/<br/><small>Run command</small>"]
        mcpctl["mcpctl/<br/><small>MCP server</small>"]
        skillsctl["skillsctl/<br/><small>Skills management</small>"]
    end

    subgraph INTERNAL["internal/"]
        dataql["dataql/<br/><small>Core engine</small>"]
        exportdata_int["exportdata/<br/><small>Export factory</small>"]
    end

    subgraph PKG["pkg/"]
        subgraph FH["filehandler/"]
            fh_csv["csv/"]
            fh_json["json/, jsonl/"]
            fh_parquet["parquet/"]
            fh_excel["excel/"]
            fh_xml["xml/, yaml/"]
            fh_avro["avro/, orc/"]
            fh_db["database/"]
            fh_mq["mq/"]
            fh_sqlite["sqlitedb/"]
        end

        subgraph DBC["dbconnector/"]
            dbc_pg["postgres.go"]
            dbc_mysql["mysql.go"]
            dbc_mongo["mongodb.go"]
            dbc_dynamo["dynamodb.go"]
            dbc_duck["duckdb.go"]
        end

        subgraph MQR["mqreader/"]
            mqr_sqs["sqs/"]
            mqr_kafka["kafka/"]
        end

        subgraph EXP["exportdata/"]
            exp_csv["csv/"]
            exp_json["json/, jsonl/"]
            exp_parquet["parquet/"]
            exp_excel["excel/"]
            exp_xml["xml/, yaml/"]
        end

        storage["storage/duckdb/<br/><small>DuckDB operations</small>"]
        repl["repl/<br/><small>Autocomplete, highlighting</small>"]

        subgraph HANDLERS["*handler/"]
            h_s3["s3handler/"]
            h_gcs["gcshandler/"]
            h_azure["azurehandler/"]
            h_url["urlhandler/"]
            h_stdin["stdinhandler/"]
        end
    end

    CMD --> INTERNAL
    INTERNAL --> PKG
```

---

## Key Operations

### Query Execution

The standard query flow when running `dataql run -f data.csv -q "SELECT ..."`:

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant CLI as CLI<br/>(dataqlctl)
    participant Engine as DataQL<br/>Engine
    participant Handler as File<br/>Handler
    participant Storage as DuckDB<br/>Storage
    participant Output as Output<br/>Formatter

    User->>CLI: dataql run -f data.csv -q "SELECT..."
    CLI->>Engine: New(params)

    Note over Engine: Input Resolution Phase
    Engine->>Engine: Resolve inputs<br/>(stdin, S3, URLs, etc.)
    Engine->>Engine: Detect format
    Engine->>Handler: Create handler for CSV

    Note over Engine: Data Loading Phase
    Engine->>Engine: Run()
    Engine->>Handler: Import()
    Handler->>Storage: BuildStructure(table, columns)

    loop For each row
        Handler->>Storage: InsertRow(table, values)
    end

    Note over Engine: Query Execution Phase
    Engine->>Storage: Query("SELECT...")
    Storage-->>Engine: *sql.Rows

    Note over Engine: Output Phase
    Engine->>Output: printResult(rows)
    Output-->>User: Formatted table

    Engine->>Engine: Cleanup temp files
```

### Interactive REPL Mode

When running DataQL without a query (`dataql run -f data.csv`):

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant REPL as REPL<br/>Interface
    participant Engine as DataQL<br/>Engine
    participant Storage as DuckDB<br/>Storage
    participant Auto as Auto-<br/>complete

    User->>Engine: dataql run -f data.csv
    Engine->>Engine: Load data into DuckDB
    Engine->>REPL: initializePrompt()

    REPL->>Auto: RefreshSchema()
    Auto->>Storage: Get tables & columns
    Storage-->>Auto: Schema info

    loop Until .exit
        REPL->>User: dataql>
        User->>REPL: Enter command/query

        alt REPL Command
            REPL->>REPL: handleREPLCommand()
            Note over REPL: .tables, .schema,<br/>.exit, etc.
        else SQL Query
            REPL->>Storage: Query(sql)
            Storage-->>REPL: Results
            REPL->>User: Display table
        end
    end

    User->>REPL: .exit
    REPL->>Engine: Close()
```

**REPL Commands:**

| Command | Description |
|---------|-------------|
| `.tables`, `\d`, `\dt` | List available tables |
| `.schema <table>` | Show table structure |
| `.count <table>` | Count rows in table |
| `.paging [on\|off]` | Toggle result pagination |
| `.pagesize <n>` | Set page size (default: 25) |
| `.timing [on\|off]` | Show query execution time |
| `.help`, `\h`, `\?` | Show help |
| `.exit`, `\q`, `.quit` | Exit REPL |

### MCP Server for LLMs

When running as an MCP server for LLM integration:

```mermaid
sequenceDiagram
    autonumber
    participant LLM as LLM<br/>(Claude, Codex, Gemini)
    participant MCP as MCP<br/>Server
    participant Engine as DataQL<br/>Engine
    participant Storage as DuckDB<br/>Storage

    Note over LLM,MCP: STDIO Communication

    LLM->>MCP: tools/list
    MCP-->>LLM: Available tools

    LLM->>MCP: tools/call dataql_query<br/>{source, query}
    MCP->>Engine: executeDataQL(params)
    Engine->>Engine: Load data
    Engine->>Storage: Query(sql)
    Storage-->>Engine: Results
    Engine-->>MCP: Table output
    MCP->>MCP: tryConvertToJSON()
    MCP-->>LLM: JSON response

    Note over LLM: Process results<br/>in context
```

**Available MCP Tools:**

| Tool | Description | Parameters |
|------|-------------|------------|
| `dataql_query` | Execute SQL query | `source`, `query`, `delimiter` |
| `dataql_schema` | Get table schema | `source` |
| `dataql_preview` | Preview first N rows | `source`, `limit` |
| `dataql_aggregate` | Run aggregation | `source`, `column`, `operation`, `group_by` |
| `dataql_mq_peek` | Peek at message queue | `source`, `max_messages`, `query` |

---

## Design Patterns

DataQL employs several design patterns for maintainability and extensibility:

| Pattern | Usage | Example |
|---------|-------|---------|
| **Factory** | Create handlers based on format | `filehandler.NewHandler(format)` |
| **Strategy** | Different import strategies per format | CSV vs JSON vs Parquet handlers |
| **Adapter** | Unified interface for databases | `dbconnector.Connector` interface |
| **Chain of Responsibility** | Input resolution pipeline | stdin → URL → S3 → GCS → Azure |
| **Repository** | Abstract storage operations | `storage.Storage` interface |
| **Command** | REPL command handling | `.tables`, `.schema`, etc. |

---

## Package Reference

| Package | Path | Responsibility |
|---------|------|----------------|
| **CLI Entry** | `cmd/` | Command-line interface and argument parsing |
| **Core Engine** | `internal/dataql/` | Main orchestration and data flow |
| **Export Factory** | `internal/exportdata/` | Route to format-specific exporters |
| **File Handlers** | `pkg/filehandler/` | Format-specific data loading |
| **DB Connectors** | `pkg/dbconnector/` | Database connection and queries |
| **MQ Readers** | `pkg/mqreader/` | Message queue peek operations |
| **DuckDB Storage** | `pkg/storage/duckdb/` | SQL execution and table management |
| **Export Formats** | `pkg/exportdata/` | Format-specific result export |
| **REPL** | `pkg/repl/` | Autocomplete and syntax highlighting |
| **Cloud Handlers** | `pkg/*handler/` | S3, GCS, Azure, URL, stdin handlers |

---

## Supported Formats

### Input Formats

| Category | Formats |
|----------|---------|
| **Files** | CSV, JSON, JSONL, XML, YAML, Parquet, Excel (.xlsx, .xls), Avro, ORC, SQLite |
| **Databases** | PostgreSQL, MySQL, DuckDB, MongoDB, DynamoDB |
| **Cloud Storage** | Amazon S3, Google Cloud Storage, Azure Blob Storage |
| **Message Queues** | AWS SQS, Apache Kafka (peek mode - non-consuming) |
| **Other** | HTTP/HTTPS URLs, Standard input (stdin) |

### Export Formats

CSV, JSON, JSONL, Excel (.xlsx), Parquet, XML, YAML

---

## See Also

- [Getting Started](getting-started.md) - Quick start guide
- [CLI Reference](cli-reference.md) - Complete command reference
- [Data Sources](data-sources.md) - Working with various data sources
- [Database Connections](databases.md) - Database connector details
- [MCP Server Setup](mcp-setup.md) - LLM integration guide
