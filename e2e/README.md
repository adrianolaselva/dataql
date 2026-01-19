# DataQL E2E Testing Infrastructure

This directory contains the complete configuration for local end-to-end testing of DataQL.

## Important: E2E Tests Must Be Run Before PRs

**E2E tests are mandatory** before submitting any Pull Request. They ensure that all data source implementations work correctly together.

```bash
# Quick workflow
make up          # Start infrastructure
make test        # Run all tests
make down        # Stop infrastructure

# Or full workflow in one command
make full        # Build, start, test, stop
```

## Services

| Service | Container | Internal Port | External Port | IP |
|---------|-----------|---------------|---------------|-----|
| PostgreSQL 16 | dataql-postgres | 5432 | **25432** | 172.28.0.10 |
| MySQL 8.0 | dataql-mysql | 3306 | **23306** | 172.28.0.11 |
| MongoDB 7.0 | dataql-mongodb | 27017 | **27117** | 172.28.0.12 |
| Zookeeper | dataql-zookeeper | 2181 | 22181 | 172.28.0.20 |
| Kafka | dataql-kafka | 9092 | **29092** | 172.28.0.21 |
| LocalStack | dataql-localstack | 4566 | **24566** | 172.28.0.30 |
| Redis 7 | dataql-redis | 6379 | **26379** | 172.28.0.40 |

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
    ├── test-postgres.sh   # PostgreSQL tests (25+ tests)
    ├── test-mysql.sh      # MySQL tests (25+ tests)
    ├── test-mongodb.sh    # MongoDB tests (20+ tests)
    ├── test-kafka.sh      # Kafka tests (10+ tests)
    ├── test-s3.sh         # S3 tests (skipped - LocalStack limitation)
    └── test-sqs.sh        # SQS tests (skipped - LocalStack limitation)
```

## Test Commands

```bash
# Run all tests (recommended)
make test

# Run specific test suite
make test-postgres    # PostgreSQL: SELECT, WHERE, ORDER BY, LIMIT, aggregates
make test-mysql       # MySQL: SELECT, WHERE, ORDER BY, LIMIT, aggregates
make test-mongodb     # MongoDB: Collections, queries, filters
make test-kafka       # Kafka: Peek mode, consumer groups, exports
make test-s3          # S3: Currently skipped (LocalStack limitation)
make test-sqs         # SQS: Currently skipped (LocalStack limitation)
```

## Test Coverage

### PostgreSQL / MySQL Tests
- Basic SELECT queries (*, specific columns, aliases)
- WHERE clause operators (=, >, <, AND, OR, IN, LIKE, BETWEEN)
- ORDER BY (ASC, DESC)
- LIMIT and OFFSET
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Export formats (CSV, JSONL, JSON)
- Multiple tables support

### MongoDB Tests
- SELECT queries on collections
- WHERE clause with comparison operators
- ORDER BY
- LIMIT
- Aggregate functions (COUNT)
- Export formats
- Multiple collections

### Kafka Tests
- Basic message reading (peek mode - non-destructive)
- SELECT body fields from JSON messages
- LIMIT message count
- Consumer group configuration
- Export formats
- Peek mode verification (non-destructive reads)

### S3/SQS Tests
Currently skipped due to LocalStack limitations:
- S3: Requires virtual-hosted-style addressing (not supported by LocalStack)
- SQS: Requires custom endpoint configuration

## Test Data

All services are initialized with consistent sample data:

**Users/test_data (5 records):**
| Name | Email | Age |
|------|-------|-----|
| Alice | alice@example.com | 28 |
| Bob | bob@example.com | 35 |
| Charlie | charlie@example.com | 42 |
| Diana | diana@example.com | 31 |
| Eve | eve@example.com | 25 |

## Infrastructure Commands

```bash
make up          # Start all services
make down        # Stop all services
make status      # Show service status
make logs        # Follow service logs
make clean       # Stop and remove volumes
make reset       # Clean and restart
make wait        # Wait for services to be healthy
```

## Shell Access

```bash
make shell-postgres  # psql session
make shell-mysql     # mysql session
make shell-mongodb   # mongosh session
make shell-redis     # redis-cli session
make shell-kafka     # Kafka container bash
```

## Running from Project Root

You can also run e2e commands from the project root:

```bash
# Using make -C
make -C e2e up
make -C e2e test
make -C e2e down

# Or using the main Makefile targets
make e2e-up
make e2e-test
make e2e-down
```

## Environment Variables

The `.env` file contains all connection URLs used by tests:

| Variable | Description |
|----------|-------------|
| `DATAQL_TEST_POSTGRES_URL` | PostgreSQL connection |
| `DATAQL_TEST_MYSQL_URL` | MySQL connection |
| `DATAQL_TEST_MONGODB_URL` | MongoDB connection |
| `DATAQL_TEST_KAFKA_URL` | Kafka connection |
| `DATAQL_TEST_S3_*` | S3 file URLs |
| `DATAQL_TEST_SQS_URL` | SQS queue URL |

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
make clean      # Remove old containers/volumes
make up         # Start fresh
```

### Check service logs
```bash
make logs                           # All services
docker-compose logs -f postgres     # Specific service
```

### Verify test data
```bash
make verify-data
```

### Port conflicts
If ports conflict with existing services, edit `docker-compose.yaml` to use different external ports.

### Test failures
1. Ensure services are healthy: `make status`
2. Check individual service logs: `docker-compose logs <service>`
3. Verify data was initialized: `make verify-data`
4. Try resetting the environment: `make reset`

## CI/CD Integration

The e2e tests are designed to be run in CI pipelines:

```yaml
# Example GitHub Actions workflow
- name: Start E2E infrastructure
  run: make e2e-up

- name: Run E2E tests
  run: make e2e-test

- name: Stop E2E infrastructure
  run: make e2e-down
  if: always()
```

---

**Remember:** Always run `make test` before submitting a PR!
