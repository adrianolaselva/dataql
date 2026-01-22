<p align="center">
  <img src="img/dataql.png" alt="DataQL" width="200">
</p>

# Data Sources

DataQL supports multiple data sources including local files, URLs, cloud storage, and stdin.

## Local Files

Load data from local files by providing the file path:

```bash
# Absolute path
dataql run -f /home/user/data/sales.csv -q "SELECT * FROM sales"

# Relative path
dataql run -f ./data/users.json -q "SELECT * FROM users"
```

### Supported Formats

| Format | Extensions | Description |
|--------|------------|-------------|
| CSV | `.csv` | Comma-separated values |
| JSON | `.json` | JSON arrays or single objects |
| JSONL | `.jsonl`, `.ndjson` | Newline-delimited JSON |
| XML | `.xml` | XML documents |
| YAML | `.yaml`, `.yml` | YAML documents |
| Parquet | `.parquet` | Apache Parquet columnar format |
| Excel | `.xlsx`, `.xls` | Microsoft Excel spreadsheets |
| Avro | `.avro` | Apache Avro format |
| ORC | `.orc` | Apache ORC format |

## HTTP/HTTPS URLs

Query data directly from web URLs:

```bash
# CSV from URL
dataql run -f "https://example.com/data.csv" -q "SELECT * FROM data"

# JSON from API
dataql run -f "https://api.example.com/users.json" -q "SELECT name, email FROM users"

# GitHub raw files
dataql run -f "https://raw.githubusercontent.com/user/repo/main/data.csv" -q "SELECT * FROM data"
```

### URL Examples

```bash
# Query public dataset
dataql run \
  -f "https://raw.githubusercontent.com/datasets/population/main/data/population.csv" \
  -q "SELECT Country_Name, Value FROM population WHERE Year = 2020 ORDER BY Value DESC LIMIT 10"

# Query JSON API
dataql run \
  -f "https://jsonplaceholder.typicode.com/posts" \
  -q "SELECT id, title FROM posts WHERE userId = 1"
```

## Standard Input (stdin)

Read data from stdin using `-` as the file path. The default table name is `stdin_data`:

```bash
# Pipe CSV data
cat data.csv | dataql run -f - -q "SELECT * FROM stdin_data"

# Pipe JSON data
echo '[{"name":"Alice","age":30},{"name":"Bob","age":25}]' | dataql run -f - -i json -q "SELECT * FROM stdin_data"

# Combine with curl
curl -s "https://api.example.com/data.json" | dataql run -f - -i json -q "SELECT * FROM stdin_data"

# Process command output
ps aux | dataql run -f - -d " " -q "SELECT * FROM stdin_data LIMIT 10"
```

### stdin with Different Formats

Use the `-i` flag to specify the input format:

```bash
# JSON from stdin
echo '{"users":[{"name":"Alice"},{"name":"Bob"}]}' | dataql run -f - -i json -q "SELECT * FROM stdin_data"

# CSV from stdin
echo -e "id,name\n1,Alice\n2,Bob" | dataql run -f - -i csv -q "SELECT * FROM stdin_data"

# Custom table name
echo -e "id,name\n1,Alice" | dataql run -f - -c people -q "SELECT * FROM people"
```

## Amazon S3

Query data stored in Amazon S3 buckets.

### Configuration

Set AWS credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

Or use AWS CLI configuration:

```bash
aws configure
```

### Usage

```bash
# S3 URL format
dataql run -f "s3://bucket-name/path/to/file.csv" -q "SELECT * FROM file"

# Query S3 object
dataql run -f "s3://my-data-bucket/sales/2024/january.csv" -q "
SELECT product, SUM(amount) as total
FROM january
GROUP BY product
ORDER BY total DESC
"

# S3 with different formats
dataql run -f "s3://my-bucket/data.json" -q "SELECT * FROM data"
dataql run -f "s3://my-bucket/data.parquet" -q "SELECT * FROM data"
```

### S3 Configuration Options

You can configure S3 access with additional options:

| Environment Variable | Description |
|---------------------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key ID |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key |
| `AWS_SESSION_TOKEN` | Session token (for temporary credentials) |
| `AWS_REGION` | AWS region (e.g., `us-east-1`) |
| `AWS_ENDPOINT_URL` | Custom endpoint (for S3-compatible storage) |

### S3-Compatible Storage

DataQL works with S3-compatible storage services like MinIO, DigitalOcean Spaces, etc.:

```bash
# MinIO
export AWS_ENDPOINT_URL="http://localhost:9000"
dataql run -f "s3://my-bucket/data.csv" -q "SELECT * FROM data"

# DigitalOcean Spaces
export AWS_ENDPOINT_URL="https://nyc3.digitaloceanspaces.com"
dataql run -f "s3://my-space/data.csv" -q "SELECT * FROM data"
```

## Google Cloud Storage (GCS)

Query data stored in Google Cloud Storage buckets.

### Configuration

Set up authentication using a service account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

Or use Application Default Credentials:

```bash
gcloud auth application-default login
```

### Usage

```bash
# GCS URL format
dataql run -f "gs://bucket-name/path/to/file.csv" -q "SELECT * FROM file"

# Query GCS object
dataql run -f "gs://my-data-bucket/analytics/events.jsonl" -q "
SELECT event_type, COUNT(*) as count
FROM events
GROUP BY event_type
"

# GCS with different formats
dataql run -f "gs://my-bucket/data.parquet" -q "SELECT * FROM data"
```

### GCS Configuration

| Environment Variable | Description |
|---------------------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON file |
| `GOOGLE_CLOUD_PROJECT` | GCP project ID (optional) |

### Service Account Setup

1. Create a service account in GCP Console
2. Download the JSON key file
3. Set the environment variable:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

## Azure Blob Storage

Query data stored in Azure Blob Storage containers.

### Configuration

Set Azure credentials via environment variables:

```bash
export AZURE_STORAGE_ACCOUNT="your-storage-account"
export AZURE_STORAGE_KEY="your-storage-key"
```

Or use Azure CLI:

```bash
az login
```

### Usage

```bash
# Azure Blob URL format
dataql run -f "az://container-name/path/to/file.csv" -q "SELECT * FROM file"

# Alternative format
dataql run -f "azure://container-name/path/to/file.csv" -q "SELECT * FROM file"

# Query Azure blob
dataql run -f "az://mycontainer/data/users.json" -q "
SELECT name, email
FROM users
WHERE active = true
"
```

### Azure Configuration

| Environment Variable | Description |
|---------------------|-------------|
| `AZURE_STORAGE_ACCOUNT` | Storage account name |
| `AZURE_STORAGE_KEY` | Storage account access key |
| `AZURE_STORAGE_CONNECTION_STRING` | Connection string (alternative) |
| `AZURE_STORAGE_SAS_TOKEN` | SAS token for limited access |

### Using Connection String

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net"
dataql run -f "az://mycontainer/data.csv" -q "SELECT * FROM data"
```

### Using SAS Token

```bash
export AZURE_STORAGE_ACCOUNT="myaccount"
export AZURE_STORAGE_SAS_TOKEN="?sv=2021-06-08&ss=b&srt=co&sp=rl..."
dataql run -f "az://mycontainer/data.csv" -q "SELECT * FROM data"
```

## Amazon DynamoDB

Query data from DynamoDB tables.

### Configuration

Set AWS credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

Or use AWS CLI configuration:

```bash
aws configure
```

### URL Format

```bash
# Basic format: region/table-name
dynamodb://us-east-1/my-table

# With custom endpoint (LocalStack, local DynamoDB)
dynamodb://us-east-1/my-table?endpoint=http://localhost:8000
```

### Usage Examples

```bash
# Query DynamoDB table
dataql run -f "dynamodb://us-east-1/users" \
  -q "SELECT name, email FROM users WHERE age > 30"

# Export to CSV
dataql run -f "dynamodb://us-east-1/orders" \
  -q "SELECT * FROM orders" \
  -e output.csv -t csv

# With LocalStack endpoint
dataql run -f "dynamodb://us-east-1/test-table?endpoint=http://localhost:4566" \
  -q "SELECT * FROM test_table"

# Custom table name
dataql run -f "dynamodb://us-east-1/my-data-table" -c my_table \
  -q "SELECT * FROM my_table"
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key ID |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key |
| `AWS_REGION` | AWS region (e.g., `us-east-1`) |
| `AWS_ENDPOINT_URL` | Custom endpoint URL (for LocalStack) |
| `AWS_ENDPOINT_URL_DYNAMODB` | DynamoDB-specific custom endpoint |

### How It Works

1. DataQL scans the DynamoDB table using the AWS SDK
2. The schema is inferred from the first item in the table
3. Data is loaded into an in-memory DuckDB database
4. You can then query the data using standard SQL syntax

### Limitations

- DynamoDB-specific query operations (KeyConditions, FilterExpressions) are not supported
- All data is loaded into memory (use `--lines` to limit rows)
- Schema is inferred from the first item; tables with inconsistent schemas may have missing columns

## Message Queues

Query messages from message queues without consuming/deleting them. Perfect for troubleshooting and debugging.

### Supported Systems

| System | URL Prefix | Status |
|--------|-----------|--------|
| AWS SQS | `sqs://` | ✅ Supported |
| Apache Kafka | `kafka://` | ✅ Supported |
| RabbitMQ | `rabbitmq://` | Coming soon |
| Apache Pulsar | `pulsar://` | Coming soon |
| Google Pub/Sub | `pubsub://` | Coming soon |

### AWS SQS

Query SQS messages without deleting them (uses VisibilityTimeout=0).

#### Configuration

Set AWS credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

#### URL Formats

```bash
# Simple format: queue name + region
sqs://my-queue-name?region=us-east-1

# Full AWS URL format (region auto-detected)
sqs://https://sqs.us-east-1.amazonaws.com/123456789/my-queue

# With options
sqs://my-queue?region=us-east-1&max_messages=50&wait_time=5
```

#### URL Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `region` | AWS region (required for simple format) | - |
| `max_messages` | Maximum messages to retrieve | 10 |
| `wait_time` | Long polling wait time in seconds (0-20) | 0 |

#### Usage Examples

```bash
# Preview messages from a queue
dataql run -f "sqs://my-events-queue?region=us-east-1" \
  -q "SELECT * FROM my_events_queue LIMIT 10"

# Analyze message types
dataql run -f "sqs://events-queue?region=us-east-1&max_messages=100" \
  -q "SELECT body_event_type, COUNT(*) as count FROM events_queue GROUP BY body_event_type"

# Filter error messages
dataql run -f "sqs://error-queue?region=us-east-1" \
  -q "SELECT message_id, body_error_message FROM error_queue WHERE body_status = 'error'"

# Check messages with timestamps
dataql run -f "sqs://orders-queue?region=us-east-1" \
  -q "SELECT message_id, timestamp, body_order_id FROM orders_queue ORDER BY timestamp DESC"
```

#### Generated Table Schema

Messages are imported into a table with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `message_id` | TEXT | SQS Message ID |
| `source` | TEXT | Queue URL |
| `timestamp` | TEXT | When message was sent |
| `timestamp_unix` | TEXT | Unix timestamp |
| `receive_count` | TEXT | Times message was received |
| `body` | TEXT | Raw message body |
| `body_*` | TEXT | Flattened JSON fields (if body is JSON) |
| `meta_*` | TEXT | Message attributes |

For JSON message bodies, fields are automatically flattened with `body_` prefix:

```json
// Message body
{"event": "order_created", "user": {"id": 123, "name": "Alice"}}

// Becomes columns
body_event = "order_created"
body_user_id = "123"
body_user_name = "Alice"
```

#### Best Practices

1. **Start with low `max_messages`** for exploration
2. **Use `wait_time` for empty queues** to wait for messages
3. **Messages are NOT deleted** - safe for troubleshooting
4. **Filter with SQL** instead of retrieving all messages

### Apache Kafka

Query Kafka topic messages without committing offsets (peek mode).

#### Configuration

Set Kafka connection via environment variables:

```bash
export KAFKA_BROKERS="localhost:9092"

# For authenticated clusters
export KAFKA_SASL_USERNAME="your-username"
export KAFKA_SASL_PASSWORD="your-password"
export KAFKA_SASL_MECHANISM="PLAIN"  # or SCRAM-SHA-256, SCRAM-SHA-512
```

#### URL Formats

```bash
# Simple format: broker/topic
kafka://localhost:9092/my-topic

# With consumer group
kafka://localhost:9092/my-topic?group=my-consumer-group

# With options
kafka://localhost:9092/my-topic?group=dataql-reader&max_messages=100
```

#### URL Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `group` | Consumer group ID | `dataql-reader` |
| `max_messages` | Maximum messages to retrieve | 100 |
| `offset` | Start offset (`newest`, `oldest`) | `newest` |

#### Usage Examples

```bash
# Preview messages from a topic
dataql run -f "kafka://localhost:9092/events" \
  -q "SELECT * FROM events LIMIT 10"

# Analyze message types
dataql run -f "kafka://localhost:9092/orders?max_messages=1000" \
  -q "SELECT body_order_type, COUNT(*) as count FROM orders GROUP BY body_order_type"

# Filter error events
dataql run -f "kafka://localhost:9092/logs" \
  -q "SELECT body_level, body_message FROM logs WHERE body_level = 'ERROR'"

# Export messages to file
dataql run -f "kafka://localhost:9092/events" \
  -q "SELECT * FROM events" \
  -e events_backup.jsonl -t jsonl
```

#### Generated Table Schema

Messages are imported into a table with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `key` | TEXT | Kafka message key |
| `partition` | TEXT | Partition number |
| `offset` | TEXT | Message offset |
| `timestamp` | TEXT | Message timestamp |
| `body` | TEXT | Raw message body |
| `body_*` | TEXT | Flattened JSON fields (if body is JSON) |

For JSON message bodies, fields are automatically flattened with `body_` prefix:

```json
// Message body
{"event": "user_signup", "user": {"id": 123, "email": "alice@example.com"}}

// Becomes columns
body_event = "user_signup"
body_user_id = "123"
body_user_email = "alice@example.com"
```

#### Peek Mode (Non-Destructive)

Kafka reads in DataQL use **peek mode**:
- Messages are read but offsets are NOT committed
- Multiple reads return the same messages
- Safe for troubleshooting and debugging
- Does not affect other consumers

### MCP Tool

When using with LLMs via MCP, the `dataql_mq_peek` tool is available:

```json
{
  "name": "dataql_mq_peek",
  "arguments": {
    "source": "sqs://my-queue?region=us-east-1",
    "max_messages": 20,
    "query": "SELECT * FROM my_queue WHERE body_status = 'error'"
  }
}
```

Also works with Kafka:

```json
{
  "name": "dataql_mq_peek",
  "arguments": {
    "source": "kafka://localhost:9092/events",
    "max_messages": 50,
    "query": "SELECT body_event_type, COUNT(*) as count FROM events GROUP BY body_event_type"
  }
}
```

---

## Multiple Sources

You can query data from multiple sources and join them:

```bash
# Join local file with S3 data
dataql run \
  -f users.csv \
  -f "s3://my-bucket/orders.csv" \
  -q "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"

# Join data from different cloud providers
dataql run \
  -f "s3://aws-bucket/products.csv" \
  -f "gs://gcp-bucket/inventory.csv" \
  -q "SELECT p.name, i.quantity FROM products p JOIN inventory i ON p.sku = i.sku"
```

## Best Practices

### Performance

1. **Use Parquet for large datasets**: Parquet is columnar and compressed, making it faster for analytical queries.

```bash
dataql run -f "s3://bucket/large-data.parquet" -q "SELECT column1, column2 FROM large_data"
```

2. **Limit records for exploration**:

```bash
dataql run -f "s3://bucket/huge-file.csv" -l 1000 -q "SELECT * FROM huge_file"
```

3. **Use JSONL for streaming**: JSONL allows processing line by line with lower memory usage.

### Security

1. **Use IAM roles in AWS**: Instead of access keys, use IAM roles when running on EC2/ECS.

2. **Use workload identity in GCP**: Configure workload identity for GKE workloads.

3. **Use managed identities in Azure**: Use managed identities for Azure VMs and AKS.

4. **Never commit credentials**: Use environment variables or secret managers.

### Troubleshooting

**S3 Access Denied:**
```bash
# Check credentials
aws sts get-caller-identity

# Check bucket permissions
aws s3 ls s3://bucket-name/
```

**GCS Permission Denied:**
```bash
# Check authentication
gcloud auth list

# Check bucket access
gsutil ls gs://bucket-name/
```

**Azure Storage Error:**
```bash
# Check account
az storage account show --name myaccount

# List containers
az storage container list --account-name myaccount
```

## See Also

- [Getting Started](getting-started.md)
- [CLI Reference](cli-reference.md)
- [Database Connections](databases.md)
- [Examples](examples.md)
