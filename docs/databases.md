<p align="center">
  <img src="img/dataql.png" alt="DataQL" width="200">
</p>

# Database Connections

DataQL can connect to various databases and query their data using SQL.

## Supported Databases

| Database | Protocol | Description |
|----------|----------|-------------|
| PostgreSQL | `postgres://` | Relational database |
| MySQL | `mysql://` | Relational database |
| DuckDB | `duckdb://` | Analytical database |
| MongoDB | `mongodb://` | Document database |
| DynamoDB | `dynamodb://` | NoSQL key-value database (AWS) |

## PostgreSQL

Connect to PostgreSQL databases to query and export data.

### Connection URL Format

```
postgres://username:password@host:port/database?table=tablename
```

### Configuration

**Using Connection URL:**

```bash
dataql run \
  -f "postgres://myuser:mypassword@localhost:5432/mydb?table=users" \
  -q "SELECT * FROM users WHERE active = true"
```

**Using Environment Variables:**

```bash
export PGHOST="localhost"
export PGPORT="5432"
export PGUSER="myuser"
export PGPASSWORD="mypassword"
export PGDATABASE="mydb"

dataql run -f "postgres://?table=users" -q "SELECT * FROM users"
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `host` | Database host | `localhost` |
| `port` | Database port | `5432` |
| `user` | Username | - |
| `password` | Password | - |
| `database` | Database name | - |
| `table` | Table to query | Required |
| `sslmode` | SSL mode | `prefer` |

### SSL Modes

| Mode | Description |
|------|-------------|
| `disable` | No SSL |
| `prefer` | Try SSL, fall back to no SSL |
| `require` | Require SSL |
| `verify-ca` | Require SSL and verify CA |
| `verify-full` | Require SSL and verify hostname |

### Examples

```bash
# Basic connection
dataql run \
  -f "postgres://user:pass@localhost:5432/mydb?table=orders" \
  -q "SELECT * FROM orders WHERE amount > 100"

# With SSL
dataql run \
  -f "postgres://user:pass@db.example.com:5432/mydb?table=users&sslmode=require" \
  -q "SELECT id, email FROM users"

# Export to CSV
dataql run \
  -f "postgres://user:pass@localhost:5432/mydb?table=products" \
  -q "SELECT * FROM products WHERE category = 'Electronics'" \
  -e products.csv -t csv

# Join with local file
dataql run \
  -f "postgres://user:pass@localhost:5432/mydb?table=customers" \
  -f orders.csv \
  -q "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id"
```

### PostgreSQL-Specific Features

**Query multiple tables:**
```bash
# Load multiple tables
dataql run \
  -f "postgres://user:pass@localhost/db?table=users" \
  -f "postgres://user:pass@localhost/db?table=orders" \
  -q "SELECT u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name"
```

## MySQL

Connect to MySQL databases.

### Connection URL Format

```
mysql://username:password@host:port/database?table=tablename
```

### Configuration

**Using Connection URL:**

```bash
dataql run \
  -f "mysql://myuser:mypassword@localhost:3306/mydb?table=products" \
  -q "SELECT * FROM products WHERE price > 50"
```

**Using Environment Variables:**

```bash
export MYSQL_HOST="localhost"
export MYSQL_PORT="3306"
export MYSQL_USER="myuser"
export MYSQL_PASSWORD="mypassword"
export MYSQL_DATABASE="mydb"

dataql run -f "mysql://?table=products" -q "SELECT * FROM products"
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `host` | Database host | `localhost` |
| `port` | Database port | `3306` |
| `user` | Username | - |
| `password` | Password | - |
| `database` | Database name | - |
| `table` | Table to query | Required |
| `charset` | Character set | `utf8mb4` |
| `parseTime` | Parse time values | `true` |
| `tls` | TLS configuration | `false` |

### Examples

```bash
# Basic connection
dataql run \
  -f "mysql://user:pass@localhost:3306/mydb?table=orders" \
  -q "SELECT * FROM orders"

# With charset
dataql run \
  -f "mysql://user:pass@localhost:3306/mydb?table=products&charset=utf8" \
  -q "SELECT * FROM products"

# Export to JSONL
dataql run \
  -f "mysql://user:pass@localhost:3306/mydb?table=logs" \
  -q "SELECT * FROM logs WHERE level = 'ERROR'" \
  -e errors.jsonl -t jsonl
```

## DuckDB

Connect to DuckDB databases for analytical workloads.

### Connection URL Format

```
duckdb:///path/to/database.db?table=tablename
```

### Configuration

**Using File Path:**

```bash
dataql run \
  -f "duckdb:///home/user/analytics.db?table=events" \
  -q "SELECT event_type, COUNT(*) FROM events GROUP BY event_type"
```

**In-Memory Database:**

```bash
dataql run \
  -f "duckdb://:memory:?table=data" \
  -q "SELECT * FROM data"
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `path` | Database file path | `:memory:` |
| `table` | Table to query | Required |
| `read_only` | Open in read-only mode | `false` |

### Examples

```bash
# Query DuckDB file
dataql run \
  -f "duckdb:///analytics.db?table=sales" \
  -q "SELECT product, SUM(amount) as total FROM sales GROUP BY product ORDER BY total DESC"

# Read-only mode
dataql run \
  -f "duckdb:///production.db?table=metrics&read_only=true" \
  -q "SELECT * FROM metrics WHERE date >= '2024-01-01'"

# Combine with other sources
dataql run \
  -f "duckdb:///warehouse.db?table=products" \
  -f inventory.csv \
  -q "SELECT p.name, i.quantity FROM products p JOIN inventory i ON p.sku = i.sku"
```

### DuckDB-Specific Features

DuckDB is optimized for analytical queries and supports:

- Columnar storage
- Vectorized query execution
- Parallel query processing
- Parquet/CSV direct querying

## MongoDB

Connect to MongoDB databases for document data.

### Connection URL Format

```
mongodb://username:password@host:port/database?collection=collectionname
```

### Configuration

**Using Connection URL:**

```bash
dataql run \
  -f "mongodb://myuser:mypassword@localhost:27017/mydb?collection=users" \
  -q "SELECT * FROM users WHERE status = 'active'"
```

**Using Environment Variables:**

```bash
export MONGODB_URI="mongodb://localhost:27017"
export MONGODB_DATABASE="mydb"

dataql run -f "mongodb://?collection=users" -q "SELECT * FROM users"
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `host` | Database host | `localhost` |
| `port` | Database port | `27017` |
| `user` | Username | - |
| `password` | Password | - |
| `database` | Database name | - |
| `collection` | Collection to query | Required |
| `authSource` | Auth database | `admin` |
| `replicaSet` | Replica set name | - |

### Document Flattening

MongoDB documents are automatically flattened for SQL queries:

**Original Document:**
```json
{
  "name": "John",
  "address": {
    "city": "New York",
    "zip": "10001"
  },
  "tags": ["developer", "golang"]
}
```

**Flattened Columns:**
- `name`
- `address_city`
- `address_zip`
- `tags` (JSON array as string)

### Examples

```bash
# Basic query
dataql run \
  -f "mongodb://user:pass@localhost:27017/mydb?collection=products" \
  -q "SELECT name, price FROM products WHERE price > 100"

# With auth source
dataql run \
  -f "mongodb://user:pass@localhost:27017/mydb?collection=users&authSource=admin" \
  -q "SELECT * FROM users"

# Replica set
dataql run \
  -f "mongodb://user:pass@host1:27017,host2:27017/mydb?collection=events&replicaSet=rs0" \
  -q "SELECT event_type, COUNT(*) FROM events GROUP BY event_type"

# Query nested fields
dataql run \
  -f "mongodb://localhost:27017/mydb?collection=orders" \
  -q "SELECT customer_name, customer_address_city, total FROM orders"

# Export to JSONL
dataql run \
  -f "mongodb://localhost:27017/logs?collection=app_logs" \
  -q "SELECT * FROM app_logs WHERE level = 'ERROR'" \
  -e errors.jsonl -t jsonl
```

### MongoDB Atlas

Connect to MongoDB Atlas:

```bash
dataql run \
  -f "mongodb+srv://user:pass@cluster.mongodb.net/mydb?collection=users" \
  -q "SELECT * FROM users"
```

## DynamoDB

Connect to Amazon DynamoDB tables.

### Connection URL Format

```
dynamodb://region/table-name
dynamodb://region/table-name?endpoint=http://localhost:8000
```

### Configuration

**Using AWS Credentials:**

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"

dataql run \
  -f "dynamodb://us-east-1/users" \
  -q "SELECT * FROM users WHERE age > 30"
```

**Using AWS CLI Profile:**

```bash
aws configure
dataql run -f "dynamodb://us-east-1/orders" -q "SELECT * FROM orders"
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `region` | AWS region (in URL host) | Required |
| `table-name` | DynamoDB table name (in URL path) | Required |
| `endpoint` | Custom endpoint URL | AWS default |

### URL Query Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `endpoint` | Custom endpoint for LocalStack/local DynamoDB | `?endpoint=http://localhost:8000` |

### Examples

```bash
# Basic query
dataql run \
  -f "dynamodb://us-east-1/products" \
  -q "SELECT name, price FROM products WHERE price > 100"

# With LocalStack endpoint
dataql run \
  -f "dynamodb://us-east-1/test-table?endpoint=http://localhost:4566" \
  -q "SELECT * FROM test_table"

# Export to CSV
dataql run \
  -f "dynamodb://us-west-2/orders" \
  -q "SELECT order_id, customer_id, total FROM orders" \
  -e orders.csv -t csv

# Custom table name in SQLite
dataql run \
  -f "dynamodb://us-east-1/my-data-table" -c my_table \
  -q "SELECT * FROM my_table LIMIT 100"

# Aggregations
dataql run \
  -f "dynamodb://us-east-1/sales" \
  -q "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY total DESC"
```

### Schema Inference

DynamoDB is schemaless, so DataQL infers the schema from the first item:

**Original DynamoDB Item:**
```json
{
  "id": {"S": "1"},
  "name": {"S": "John"},
  "age": {"N": "30"},
  "active": {"BOOL": true}
}
```

**Flattened Columns:**
- `id` (TEXT)
- `name` (TEXT)
- `age` (TEXT)
- `active` (TEXT)

### Environment Variables

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key ID |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key |
| `AWS_SESSION_TOKEN` | Session token (for temporary credentials) |
| `AWS_REGION` | Default AWS region |
| `AWS_ENDPOINT_URL` | Custom endpoint (for LocalStack) |
| `AWS_ENDPOINT_URL_DYNAMODB` | DynamoDB-specific endpoint |

### LocalStack Integration

For local development and testing:

```bash
# Start LocalStack
docker run -d -p 4566:4566 localstack/localstack

# Set environment
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_ENDPOINT_URL=http://localhost:4566

# Create table
aws --endpoint-url=$AWS_ENDPOINT_URL dynamodb create-table \
  --table-name users \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Query with DataQL
dataql run \
  -f "dynamodb://us-east-1/users?endpoint=http://localhost:4566" \
  -q "SELECT * FROM users"
```

### Limitations

1. **No native DynamoDB queries**: DataQL scans the table and loads data into SQLite
2. **Memory usage**: Large tables are fully loaded into memory
3. **Consistent schema**: Tables with varying item structures may have missing columns
4. **Read-only**: DataQL only reads data, doesn't write to DynamoDB

## Security Best Practices

### 1. Use Environment Variables

Never hardcode credentials in commands:

```bash
# Good
export DB_PASSWORD="secret"
dataql run -f "postgres://user:$DB_PASSWORD@localhost/db?table=users" -q "SELECT * FROM users"

# Bad - password visible in history
dataql run -f "postgres://user:secret@localhost/db?table=users" -q "SELECT * FROM users"
```

### 2. Use Read-Only Users

Create database users with minimal permissions:

```sql
-- PostgreSQL
CREATE USER dataql_reader WITH PASSWORD 'password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dataql_reader;

-- MySQL
CREATE USER 'dataql_reader'@'%' IDENTIFIED BY 'password';
GRANT SELECT ON mydb.* TO 'dataql_reader'@'%';
```

### 3. Use SSL/TLS

Always use encrypted connections in production:

```bash
# PostgreSQL with SSL
dataql run -f "postgres://user:pass@host/db?table=t&sslmode=require" -q "SELECT * FROM t"

# MySQL with TLS
dataql run -f "mysql://user:pass@host/db?table=t&tls=true" -q "SELECT * FROM t"
```

### 4. Limit Network Access

- Use VPC/private networks when possible
- Configure firewall rules to allow only necessary connections
- Use SSH tunnels for remote databases

```bash
# SSH tunnel example
ssh -L 5432:db.internal:5432 user@bastion &
dataql run -f "postgres://user:pass@localhost:5432/db?table=users" -q "SELECT * FROM users"
```

## Troubleshooting

### Connection Refused

```bash
# Check if database is running
pg_isready -h localhost -p 5432

# Check network connectivity
telnet localhost 5432
```

### Authentication Failed

```bash
# Verify credentials
psql "postgres://user:pass@localhost/db" -c "SELECT 1"

# Check user permissions
mysql -u user -p -e "SHOW GRANTS"
```

### SSL Certificate Error

```bash
# PostgreSQL - disable SSL for testing
dataql run -f "postgres://...?sslmode=disable" -q "..."

# Download CA certificate
curl -o ca.pem https://example.com/ca.pem
export PGSSLROOTCERT=ca.pem
```

### Timeout Errors

```bash
# Increase connection timeout
dataql run -f "postgres://...?connect_timeout=30" -q "..."
```

## See Also

- [Getting Started](getting-started.md)
- [CLI Reference](cli-reference.md)
- [Data Sources](data-sources.md)
- [Examples](examples.md)
