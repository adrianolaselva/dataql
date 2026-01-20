# Getting Started with DataQL

This guide will help you get up and running with DataQL in minutes.

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

### Install Specific Version

```bash
# Linux/macOS
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --version v0.1.0

# Windows
$env:DATAQL_VERSION="v0.1.0"; irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
```

### User Installation (No sudo required)

```bash
# Linux/macOS - Installs to ~/.local/bin
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --local

# Windows
$env:DATAQL_USER_INSTALL="true"; irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.ps1 | iex
```

### Build from Source

```bash
git clone https://github.com/adrianolaselva/dataql.git
cd dataql
make build
make install       # requires sudo
# or
make install-local # installs to ~/.local/bin
```

### Verify Installation

```bash
dataql --version
```

### Updating DataQL

**Upgrade to latest version:**

```bash
# Linux/macOS - Only upgrades if a newer version is available
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --upgrade

# Force reinstall (even if same version)
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --force
```

**Clean install (remove all versions first):**

```bash
# Remove all existing installations and install fresh
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/install.sh | bash -s -- --clean --force
```

### Uninstalling DataQL

**Linux / macOS:**

```bash
curl -fsSL https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/uninstall.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/adrianolaselva/dataql/main/scripts/uninstall.ps1 | iex
```

**Manual uninstall:**

```bash
# Remove system installation
sudo rm /usr/local/bin/dataql

# Remove user installation
rm ~/.local/bin/dataql
```

## Hello World Examples

### Example 1: Query a CSV File

Create a sample CSV file:

```bash
cat > users.csv << 'EOF'
id,name,email,age,city
1,Alice,alice@example.com,28,New York
2,Bob,bob@example.com,35,Los Angeles
3,Charlie,charlie@example.com,42,Chicago
4,Diana,diana@example.com,31,Houston
5,Eve,eve@example.com,25,Phoenix
EOF
```

Query the data:

```bash
# Select all records
dataql run -f users.csv -q "SELECT * FROM users"

# Filter by age
dataql run -f users.csv -q "SELECT name, city FROM users WHERE age > 30"

# Count records by city
dataql run -f users.csv -q "SELECT city, COUNT(*) as count FROM users GROUP BY city"
```

### Example 2: Query a JSON File

Create a sample JSON file:

```bash
cat > products.json << 'EOF'
[
  {"id": 1, "name": "Laptop", "category": "Electronics", "price": 999.99, "stock": 50},
  {"id": 2, "name": "Mouse", "category": "Electronics", "price": 29.99, "stock": 200},
  {"id": 3, "name": "Desk", "category": "Furniture", "price": 299.99, "stock": 30},
  {"id": 4, "name": "Chair", "category": "Furniture", "price": 199.99, "stock": 45},
  {"id": 5, "name": "Monitor", "category": "Electronics", "price": 399.99, "stock": 75}
]
EOF
```

Query the data:

```bash
# List all products
dataql run -f products.json -q "SELECT * FROM products"

# Calculate total value per category
dataql run -f products.json -q "SELECT category, SUM(price * stock) as total_value FROM products GROUP BY category"

# Find products with low stock
dataql run -f products.json -q "SELECT name, stock FROM products WHERE stock < 50 ORDER BY stock"
```

### Example 3: Query a JSONL File (Newline-Delimited JSON)

Create a sample JSONL file:

```bash
cat > events.jsonl << 'EOF'
{"timestamp": "2024-01-15T10:30:00Z", "level": "INFO", "message": "Application started"}
{"timestamp": "2024-01-15T10:30:05Z", "level": "DEBUG", "message": "Loading configuration"}
{"timestamp": "2024-01-15T10:30:10Z", "level": "INFO", "message": "Connected to database"}
{"timestamp": "2024-01-15T10:31:00Z", "level": "ERROR", "message": "Failed to process request"}
{"timestamp": "2024-01-15T10:31:05Z", "level": "WARN", "message": "High memory usage detected"}
EOF
```

Query the data:

```bash
# Show all events
dataql run -f events.jsonl -q "SELECT * FROM events"

# Filter by log level
dataql run -f events.jsonl -q "SELECT timestamp, message FROM events WHERE level = 'ERROR'"

# Count events by level
dataql run -f events.jsonl -q "SELECT level, COUNT(*) as count FROM events GROUP BY level ORDER BY count DESC"
```

### Example 4: Interactive Mode (REPL)

Start DataQL in interactive mode:

```bash
dataql run -f users.csv
```

You'll see the DataQL prompt:

```
dataql>
```

Now you can run multiple queries:

```sql
dataql> SELECT * FROM users;
dataql> SELECT AVG(age) as avg_age FROM users;
dataql> SELECT city, COUNT(*) FROM users GROUP BY city;
dataql> .tables
dataql> .schema users
dataql> .exit
```

### Example 5: Export Results

Export query results to different formats:

```bash
# Export to CSV
dataql run -f products.json -q "SELECT * FROM products WHERE category = 'Electronics'" -e electronics.csv -t csv

# Export to JSONL
dataql run -f users.csv -q "SELECT name, email FROM users" -e users_export.jsonl -t jsonl
```

### Example 6: Join Multiple Files

Create two related files:

```bash
cat > orders.csv << 'EOF'
order_id,customer_id,product,amount
1,1,Laptop,1
2,2,Mouse,3
3,1,Monitor,2
4,3,Chair,1
EOF

cat > customers.csv << 'EOF'
customer_id,name,country
1,Alice,USA
2,Bob,Canada
3,Charlie,UK
EOF
```

Join the data:

```bash
dataql run -f orders.csv -f customers.csv -q "
SELECT
    o.order_id,
    c.name as customer_name,
    o.product,
    o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
"
```

### Example 7: Query from URL

Query data directly from a URL:

```bash
# Query CSV from URL
dataql run -f "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv" \
  -q "SELECT name, municipality, iso_country FROM airport_codes WHERE iso_country = 'BR' LIMIT 10"
```

### Example 8: Read from stdin

Pipe data directly to DataQL:

```bash
# Pipe CSV data
cat users.csv | dataql run -f - -q "SELECT * FROM stdin WHERE age > 30"

# Pipe JSON data
echo '[{"a":1},{"a":2}]' | dataql run -f - -q "SELECT * FROM stdin"

# Combine with other tools
curl -s "https://api.example.com/data.json" | dataql run -f - -q "SELECT * FROM stdin"
```

## Next Steps

- [CLI Reference](cli-reference.md) - Complete command-line reference
- [Data Sources](data-sources.md) - Working with S3, GCS, Azure, and URLs
- [Database Connections](databases.md) - Connect to PostgreSQL, MySQL, DuckDB, MongoDB
- [Examples](examples.md) - Real-world usage examples
