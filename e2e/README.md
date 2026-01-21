# DataQL E2E Testing Infrastructure

This directory contains the complete configuration for local end-to-end testing of DataQL.

## Important: E2E Tests Must Be Run Before PRs

**E2E tests are mandatory** before submitting any Pull Request. They ensure that all data source implementations work correctly together.

```bash
# Quick workflow
make e2e-up          # Start infrastructure
make e2e-test        # Run all tests
make e2e-down        # Stop infrastructure

# Or from e2e directory
cd e2e
make up && make test && make down
```

## Services

| Service | Container | Internal Port | External Port | IP | Purpose |
|---------|-----------|---------------|---------------|-----|---------|
| PostgreSQL 16 | dataql-postgres | 5432 | **25432** | 172.28.0.10 | SQL database testing |
| MySQL 8.0 | dataql-mysql | 3306 | **23306** | 172.28.0.11 | SQL database testing |
| MongoDB 7.0 | dataql-mongodb | 27017 | **27117** | 172.28.0.12 | NoSQL database testing |
| Zookeeper | dataql-zookeeper | 2181 | 22181 | 172.28.0.20 | Kafka dependency |
| Kafka | dataql-kafka | 9092 | **29092** | 172.28.0.21 | Message queue testing |
| LocalStack | dataql-localstack | 4566 | **24566** | 172.28.0.30 | AWS S3/SQS emulation |
| Redis 7 | dataql-redis | 6379 | **26379** | 172.28.0.40 | Future use |

## Network

Uses custom subnet `172.28.0.0/16` to avoid conflicts with common Docker networks.

## Directory Structure

```
e2e/
├── Makefile               # E2E testing commands
├── docker-compose.yaml    # Docker Compose configuration
├── .env                   # Environment variables for tests
├── README.md              # This file
├── scripts/               # Initialization scripts
│   ├── init-postgres.sql  # PostgreSQL schema and data
│   ├── init-mysql.sql     # MySQL schema and data
│   ├── init-mongodb.js    # MongoDB collections and data
│   └── init-localstack.sh # S3/SQS/DynamoDB setup
└── tests/                 # Test scripts
    ├── test-all.sh        # Run all test suites
    ├── test-postgres.sh   # PostgreSQL tests (26 tests)
    ├── test-mysql.sh      # MySQL tests (26 tests)
    ├── test-mongodb.sh    # MongoDB tests (20+ tests)
    ├── test-kafka.sh      # Kafka tests (10+ tests)
    ├── test-s3.sh         # S3 tests (13 tests)
    └── test-sqs.sh        # SQS tests (16 tests)
```

## Test Commands

```bash
# Run all tests (recommended)
make e2e-test-scripts

# Run specific test suite from project root
make e2e-test-postgres    # PostgreSQL: SELECT, WHERE, ORDER BY, LIMIT, aggregates
make e2e-test-mysql       # MySQL: SELECT, WHERE, ORDER BY, LIMIT, aggregates
make e2e-test-mongodb     # MongoDB: Collections, queries, filters
make e2e-test-kafka       # Kafka: Peek mode, consumer groups, exports
make e2e-test-s3          # S3: CSV, JSON, JSONL from LocalStack S3
make e2e-test-sqs         # SQS: Message queue reading from LocalStack
```

## Test Coverage Summary

| Suite | Tests | Coverage |
|-------|-------|----------|
| PostgreSQL | 26 | SELECT, WHERE, ORDER BY, LIMIT, aggregates, exports |
| MySQL | 26 | SELECT, WHERE, ORDER BY, LIMIT, aggregates, exports |
| MongoDB | 20+ | Collections, queries, filters, exports |
| Kafka | 10+ | Peek mode, message parsing, exports |
| S3 | 13 | CSV/JSON/JSONL file reading, queries, exports |
| SQS | 16 | Message reading, filtering, aggregation, exports |

### PostgreSQL / MySQL Tests
- Basic SELECT queries (*, specific columns, aliases)
- WHERE clause operators (=, >, <, >=, <=, AND, OR, IN, LIKE, BETWEEN)
- ORDER BY (ASC, DESC)
- LIMIT and OFFSET
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Export formats (CSV, JSONL, JSON)
- Multiple tables support (users, departments, test_data)

### MongoDB Tests
- SELECT queries on collections
- WHERE clause with comparison operators
- ORDER BY
- LIMIT
- Aggregate functions (COUNT)
- Export formats
- Multiple collections (users, orders)

### Kafka Tests
- Basic message reading (peek mode - non-destructive)
- SELECT body fields from JSON messages
- LIMIT message count
- Consumer group configuration
- Export formats (CSV, JSONL, JSON)
- Peek mode verification (non-destructive reads)

### S3 Tests (LocalStack)
- **CSV File Tests**: Basic read, SELECT columns, WHERE clause, COUNT, LIMIT
- **JSON File Tests**: Read JSON arrays, WHERE clause filtering
- **JSONL File Tests**: Read JSONL, ORDER BY
- **Export Tests**: Export to CSV, JSONL, JSON formats

### SQS Tests (LocalStack)
- **Message Reading**: Basic peek, SELECT specific columns (`body_*` fields)
- **Filtering**: WHERE clause on message body fields
- **Aggregation**: COUNT with and without WHERE
- **Ordering**: ORDER BY ASC/DESC
- **Limiting**: LIMIT clause
- **Export**: Export to CSV, JSONL, JSON formats

**Note on SQS Column Names**: Message body fields are prefixed with `body_` (e.g., `body_name`, `body_email`, `body_age`). This is because messages are flattened from JSON into SQL-compatible columns.

## Test Data

All services are initialized with consistent sample data (5 records):

| Name | Email | Age |
|------|-------|-----|
| Alice | alice@example.com | 28 |
| Bob | bob@example.com | 35 |
| Charlie | charlie@example.com | 42 |
| Diana | diana@example.com | 31 |
| Eve | eve@example.com | 25 |

**Data Locations:**
- PostgreSQL/MySQL: `test_data`, `users`, `departments` tables
- MongoDB: `users`, `orders` collections
- Kafka: `dataql-test-topic` with JSON messages
- S3: `s3://dataql-test-bucket/fixtures/` (simple.csv, people.json, data.jsonl)
- SQS: `dataql-test-queue` with JSON messages

## Infrastructure Commands

```bash
# From project root
make e2e-up          # Start all services
make e2e-down        # Stop all services
make e2e-status      # Show service status
make e2e-logs        # Follow service logs
make e2e-clean       # Stop and remove volumes
make e2e-reset       # Clean and restart
make e2e-wait        # Wait for services to be healthy

# From e2e directory
cd e2e
make up
make down
make status
```

## Shell Access

```bash
make e2e-shell-postgres  # psql session
make e2e-shell-mysql     # mysql session
make e2e-shell-mongodb   # mongosh session
make e2e-shell-redis     # redis-cli session
```

## Running Tests

### From Project Root (Recommended)

```bash
# Run all shell-based e2e tests
make e2e-test-scripts

# Run individual test suites
make e2e-test-postgres
make e2e-test-mysql
make e2e-test-mongodb
make e2e-test-kafka
make e2e-test-s3
make e2e-test-sqs
```

### From e2e Directory

```bash
cd e2e
make test           # Run all tests
make test-postgres  # PostgreSQL only
make test-s3        # S3 only
```

## Environment Variables

The `.env` file contains all connection URLs used by tests:

| Variable | Description | Example |
|----------|-------------|---------|
| `DATAQL_TEST_POSTGRES_URL` | PostgreSQL connection | `postgres://dataql:dataql_pass@localhost:25432/dataql_test/test_data` |
| `DATAQL_TEST_MYSQL_URL` | MySQL connection | `mysql://dataql:dataql_pass@localhost:23306/dataql_test/test_data` |
| `DATAQL_TEST_MONGODB_URL` | MongoDB connection | `mongodb://dataql:dataql_pass@localhost:27117/dataql_test/users` |
| `DATAQL_TEST_KAFKA_URL` | Kafka connection | `kafka://localhost:29092/dataql-test-topic` |
| `DATAQL_TEST_S3_CSV` | S3 CSV file | `s3://dataql-test-bucket/fixtures/simple.csv` |
| `DATAQL_TEST_S3_JSON` | S3 JSON file | `s3://dataql-test-bucket/fixtures/people.json` |
| `DATAQL_TEST_S3_JSONL` | S3 JSONL file | `s3://dataql-test-bucket/fixtures/data.jsonl` |
| `DATAQL_TEST_SQS_URL` | SQS queue URL | `sqs://http://localhost:24566/000000000000/dataql-test-queue` |
| `AWS_ENDPOINT_URL` | LocalStack endpoint | `http://localhost:24566` |
| `AWS_ACCESS_KEY_ID` | AWS credentials | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials | `test` |

## Adding New Tests

When adding new functionality to DataQL:

1. Create test cases in the appropriate `tests/test-*.sh` file
2. Follow the existing test structure:
   ```bash
   test_your_feature() {
       log_info "Test: Your test description"
       result=$($DATAQL_BIN run -q "YOUR QUERY" -f "$URL" 2>&1)
       if echo "$result" | grep -q "expected"; then
           log_pass "Feature works correctly"
       else
           log_fail "Feature failed" "$result"
       fi
   }
   ```
3. Add the test function call in the appropriate section
4. Run the full test suite to ensure no regressions

## Troubleshooting

### Services not starting
```bash
make e2e-clean      # Remove old containers/volumes
make e2e-up         # Start fresh
```

### Check service logs
```bash
make e2e-logs                              # All services
docker-compose -f e2e/docker-compose.yaml logs -f postgres  # Specific service
```

### Kafka cluster ID mismatch
If Kafka fails with "InconsistentClusterIdException":
```bash
make e2e-clean   # Remove volumes including Kafka data
make e2e-up      # Start fresh
```

### Port conflicts
If ports conflict with existing services, the following ports are used:
- PostgreSQL: 25432
- MySQL: 23306
- MongoDB: 27117
- Kafka: 29092
- LocalStack: 24566
- Redis: 26379

Edit `e2e/docker-compose.yaml` to change external ports if needed.

### Test failures
1. Ensure services are healthy: `make e2e-status`
2. Check individual service logs
3. Try resetting the environment: `make e2e-reset`

### LocalStack S3/SQS Issues

**S3 Path-Style Addressing**: DataQL uses path-style addressing for S3 (`o.UsePathStyle = true`) which is required for LocalStack compatibility.

**SQS Full URL Format**: The SQS URL must use the full URL format for LocalStack:
```
sqs://http://localhost:24566/000000000000/queue-name
```
The simple format (`sqs://queue-name?region=X`) calls AWS `GetQueueUrl` which returns internal Docker network URLs not accessible from the host.

**SQS Message Consumption**: LocalStack doesn't fully support `VisibilityTimeout=0`, so messages may be consumed during reads. The e2e tests repopulate messages before each test to handle this limitation.

## Known Limitations

### LocalStack
- **SQS VisibilityTimeout=0**: Not fully supported. Messages are consumed despite setting VisibilityTimeout=0 in peek mode.
- **SQS GetQueueUrl**: Returns internal Docker URLs. Use full URL format instead.

## CI/CD Integration

The e2e tests are designed to be run in CI pipelines:

```yaml
# Example GitHub Actions workflow
jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.24'

      - name: Build
        run: make build

      - name: Start E2E infrastructure
        run: make e2e-up

      - name: Wait for services
        run: make e2e-wait

      - name: Run E2E tests
        run: make e2e-test-scripts

      - name: Stop E2E infrastructure
        run: make e2e-down
        if: always()
```

## Adding New Tests

When adding new functionality to DataQL:

1. Create test cases in the appropriate `tests/test-*.sh` file
2. Follow the existing test structure:
   ```bash
   test_your_feature() {
       log_info "Test: Your test description"
       result=$($DATAQL_BIN run -q "YOUR QUERY" -f "$URL" 2>&1)
       if echo "$result" | grep -q "expected"; then
           log_pass "Feature works correctly"
       else
           log_fail "Feature failed" "$result"
       fi
   }
   ```
3. Add the test function call in the appropriate section
4. Run the full test suite to ensure no regressions:
   ```bash
   make e2e-test-scripts
   ```

---

**Remember:** Always run `make e2e-test-scripts` before submitting a PR!
