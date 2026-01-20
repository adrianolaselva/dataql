<p align="center">
  <img src="img/dataql.png" alt="DataQL" width="200">
</p>

# Examples

Real-world examples of using DataQL for various data processing tasks.

## Data Analysis

### Sales Analysis

Analyze sales data to find top products and revenue:

```bash
# Create sample sales data
cat > sales.csv << 'EOF'
date,product,category,quantity,price,region
2024-01-15,Laptop,Electronics,5,999.99,North
2024-01-15,Mouse,Electronics,20,29.99,North
2024-01-16,Desk,Furniture,3,299.99,South
2024-01-16,Chair,Furniture,8,199.99,South
2024-01-17,Monitor,Electronics,10,399.99,East
2024-01-17,Keyboard,Electronics,15,89.99,West
2024-01-18,Laptop,Electronics,3,999.99,South
2024-01-18,Desk,Furniture,2,299.99,North
EOF

# Total revenue by category
dataql run -f sales.csv -q "
SELECT
    category,
    SUM(quantity * price) as total_revenue,
    SUM(quantity) as units_sold
FROM sales
GROUP BY category
ORDER BY total_revenue DESC
"

# Top 5 products by revenue
dataql run -f sales.csv -q "
SELECT
    product,
    SUM(quantity * price) as revenue
FROM sales
GROUP BY product
ORDER BY revenue DESC
LIMIT 5
"

# Daily sales trend
dataql run -f sales.csv -q "
SELECT
    date,
    COUNT(*) as transactions,
    SUM(quantity * price) as daily_revenue
FROM sales
GROUP BY date
ORDER BY date
"

# Regional performance
dataql run -f sales.csv -q "
SELECT
    region,
    category,
    SUM(quantity * price) as revenue
FROM sales
GROUP BY region, category
ORDER BY region, revenue DESC
"
```

### Log Analysis

Analyze application logs to find errors and patterns:

```bash
# Create sample log data
cat > app.jsonl << 'EOF'
{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","service":"api","message":"Server started","user_id":null}
{"timestamp":"2024-01-15T10:30:05Z","level":"DEBUG","service":"api","message":"Loading config","user_id":null}
{"timestamp":"2024-01-15T10:31:00Z","level":"ERROR","service":"api","message":"Database connection failed","user_id":null}
{"timestamp":"2024-01-15T10:31:30Z","level":"INFO","service":"auth","message":"User login","user_id":"user123"}
{"timestamp":"2024-01-15T10:32:00Z","level":"WARN","service":"api","message":"High memory usage","user_id":null}
{"timestamp":"2024-01-15T10:32:30Z","level":"ERROR","service":"auth","message":"Invalid token","user_id":"user456"}
{"timestamp":"2024-01-15T10:33:00Z","level":"INFO","service":"api","message":"Request processed","user_id":"user123"}
{"timestamp":"2024-01-15T10:33:30Z","level":"ERROR","service":"api","message":"Timeout error","user_id":"user789"}
EOF

# Count errors by service
dataql run -f app.jsonl -q "
SELECT
    service,
    COUNT(*) as error_count
FROM app
WHERE level = 'ERROR'
GROUP BY service
ORDER BY error_count DESC
"

# Find all errors with details
dataql run -f app.jsonl -q "
SELECT timestamp, service, message
FROM app
WHERE level = 'ERROR'
ORDER BY timestamp
"

# Log level distribution
dataql run -f app.jsonl -q "
SELECT
    level,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM app), 2) as percentage
FROM app
GROUP BY level
ORDER BY count DESC
"

# User activity
dataql run -f app.jsonl -q "
SELECT
    user_id,
    COUNT(*) as actions
FROM app
WHERE user_id IS NOT NULL
GROUP BY user_id
"
```

## Data Transformation

### CSV to JSONL Conversion

Convert CSV files to JSONL format:

```bash
# Convert entire file
dataql run -f data.csv -q "SELECT * FROM data" -e data.jsonl -t jsonl

# Convert with filtering
dataql run -f users.csv \
    -q "SELECT id, name, email FROM users WHERE active = 1" \
    -e active_users.jsonl -t jsonl

# Convert with transformation
dataql run -f orders.csv \
    -q "SELECT id, customer_id, total, date, 'processed' as status FROM orders" \
    -e processed_orders.jsonl -t jsonl
```

### JSON Flattening

Flatten nested JSON structures:

```bash
# Create nested JSON
cat > nested.json << 'EOF'
[
  {
    "user": {
      "id": 1,
      "profile": {
        "name": "Alice",
        "contact": {
          "email": "alice@example.com",
          "phone": "555-1234"
        }
      }
    },
    "orders": 5
  },
  {
    "user": {
      "id": 2,
      "profile": {
        "name": "Bob",
        "contact": {
          "email": "bob@example.com",
          "phone": "555-5678"
        }
      }
    },
    "orders": 3
  }
]
EOF

# Query flattened data
dataql run -f nested.json -q "
SELECT
    user_id,
    user_profile_name as name,
    user_profile_contact_email as email,
    orders
FROM nested
"
```

### Data Aggregation

Aggregate data from multiple files:

```bash
# Create sample files
cat > jan_sales.csv << 'EOF'
product,amount
Laptop,5000
Mouse,500
Keyboard,300
EOF

cat > feb_sales.csv << 'EOF'
product,amount
Laptop,6000
Mouse,600
Monitor,2000
EOF

# Combine and aggregate
dataql run -f jan_sales.csv -f feb_sales.csv -q "
SELECT
    product,
    SUM(amount) as total
FROM (
    SELECT product, amount FROM jan_sales
    UNION ALL
    SELECT product, amount FROM feb_sales
)
GROUP BY product
ORDER BY total DESC
"
```

## ETL Pipelines

### Extract from PostgreSQL, Transform, Load to JSONL

```bash
# Extract and transform data from PostgreSQL
dataql run \
    -f "postgres://user:pass@localhost:5432/db?table=orders" \
    -q "
    SELECT
        o.id,
        o.customer_id,
        o.total,
        o.created_at,
        CASE
            WHEN o.total > 1000 THEN 'high'
            WHEN o.total > 100 THEN 'medium'
            ELSE 'low'
        END as value_tier
    FROM orders o
    WHERE o.status = 'completed'
    " \
    -e processed_orders.jsonl -t jsonl
```

### Merge Multiple Data Sources

```bash
# Merge data from S3 and local file
dataql run \
    -f "s3://my-bucket/customers.csv" \
    -f orders.csv \
    -q "
    SELECT
        c.name as customer_name,
        c.email,
        COUNT(o.id) as order_count,
        SUM(o.total) as total_spent
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    GROUP BY c.id, c.name, c.email
    HAVING order_count > 0
    ORDER BY total_spent DESC
    " \
    -e customer_summary.csv -t csv
```

### Data Quality Checks

```bash
# Check for missing values
dataql run -f data.csv -q "
SELECT
    COUNT(*) as total_rows,
    SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_email,
    SUM(CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END) as missing_phone
FROM data
"

# Find duplicates
dataql run -f users.csv -q "
SELECT email, COUNT(*) as count
FROM users
GROUP BY email
HAVING count > 1
"

# Validate data ranges
dataql run -f products.csv -q "
SELECT *
FROM products
WHERE price < 0 OR price > 10000
"
```

## Working with APIs

### Query JSON API Response

```bash
# Query GitHub API
dataql run \
    -f "https://api.github.com/repos/golang/go/releases" \
    -q "SELECT tag_name, name, published_at FROM releases LIMIT 5"

# Query and filter
dataql run \
    -f "https://jsonplaceholder.typicode.com/posts" \
    -q "SELECT id, title FROM posts WHERE userId = 1"
```

### Process curl Output

```bash
# Pipe API response
curl -s "https://api.github.com/users/octocat/repos" | \
    dataql run -f - -q "
    SELECT name, stargazers_count, language
    FROM stdin
    WHERE language IS NOT NULL
    ORDER BY stargazers_count DESC
    LIMIT 10
    "
```

## Cloud Data Processing

### S3 Data Lake Query

```bash
# Query Parquet files in S3
dataql run \
    -f "s3://my-datalake/events/2024/01/events.parquet" \
    -q "
    SELECT
        event_type,
        COUNT(*) as count,
        AVG(duration_ms) as avg_duration
    FROM events
    GROUP BY event_type
    ORDER BY count DESC
    "
```

### Cross-Cloud Data Join

```bash
# Join S3 and GCS data
dataql run \
    -f "s3://aws-bucket/products.csv" \
    -f "gs://gcp-bucket/inventory.csv" \
    -q "
    SELECT
        p.name,
        p.price,
        i.quantity,
        i.warehouse
    FROM products p
    JOIN inventory i ON p.sku = i.sku
    WHERE i.quantity < 10
    ORDER BY i.quantity
    "
```

## Interactive Analysis

### Start REPL for Exploration

```bash
# Load data and start interactive mode
dataql run -f sales.csv

# In REPL:
dataql> .tables
dataql> .schema sales
dataql> SELECT * FROM sales LIMIT 5;
dataql> SELECT category, COUNT(*) FROM sales GROUP BY category;
dataql> .exit
```

### Multi-Table Exploration

```bash
# Load multiple tables for interactive exploration
dataql run -f users.csv -f orders.csv -f products.csv

# In REPL:
dataql> .tables
Tables: users, orders, products

dataql> SELECT u.name, COUNT(o.id) as orders
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        GROUP BY u.id;
```

## Performance Optimization

### Query Large Files Efficiently

```bash
# Limit rows for exploration
dataql run -f huge_file.csv -l 1000 -q "SELECT * FROM huge_file"

# Use Parquet for large datasets
dataql run -f "s3://bucket/large_data.parquet" -q "
SELECT column1, column2
FROM large_data
WHERE date >= '2024-01-01'
LIMIT 10000
"
```

### Persist for Multiple Queries

```bash
# Save to SQLite for repeated queries
dataql run -f large_data.csv -s ./data.db

# Later, query the SQLite directly
sqlite3 ./data.db "SELECT * FROM large_data WHERE condition"
```

## Automation Scripts

### Daily Report Script

```bash
#!/bin/bash
# daily_report.sh

DATE=$(date +%Y-%m-%d)
OUTPUT_DIR="./reports/$DATE"
mkdir -p "$OUTPUT_DIR"

# Generate sales summary
dataql run \
    -f "postgres://user:pass@localhost/db?table=orders" \
    -q "
    SELECT
        category,
        COUNT(*) as orders,
        SUM(total) as revenue
    FROM orders
    WHERE date = '$DATE'
    GROUP BY category
    " \
    -e "$OUTPUT_DIR/sales_summary.csv" -t csv

# Generate error report
dataql run \
    -f "s3://logs-bucket/app/$DATE/logs.jsonl" \
    -q "
    SELECT service, message, COUNT(*) as count
    FROM logs
    WHERE level = 'ERROR'
    GROUP BY service, message
    ORDER BY count DESC
    " \
    -e "$OUTPUT_DIR/errors.jsonl" -t jsonl

echo "Reports generated in $OUTPUT_DIR"
```

### Data Sync Script

```bash
#!/bin/bash
# sync_data.sh

# Export from production PostgreSQL
dataql run \
    -f "postgres://readonly:pass@prod-db:5432/app?table=users" \
    -q "SELECT id, email, created_at FROM users WHERE updated_at > '$LAST_SYNC'" \
    -e /tmp/users_delta.jsonl -t jsonl

# Upload to S3
aws s3 cp /tmp/users_delta.jsonl "s3://data-lake/users/$(date +%Y%m%d).jsonl"

# Update sync timestamp
echo $(date -u +%Y-%m-%dT%H:%M:%SZ) > /var/lib/sync/last_sync
```

## See Also

- [Getting Started](getting-started.md)
- [CLI Reference](cli-reference.md)
- [Data Sources](data-sources.md)
- [Database Connections](databases.md)
