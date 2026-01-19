# DataQL E2E Testing Infrastructure

This directory contains the complete configuration for local end-to-end testing of DataQL.

## Quick Start

```bash
# From e2e directory
make up          # Start infrastructure
make test        # Run all tests
make down        # Stop infrastructure

# Or full workflow
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
│   ├── init-postgres.sql
│   ├── init-mysql.sql
│   ├── init-mongodb.js
│   └── init-localstack.sh
└── tests/                 # Test scripts
    ├── test-all.sh        # Run all test suites
    ├── test-postgres.sh   # PostgreSQL tests
    ├── test-mysql.sh      # MySQL tests
    ├── test-mongodb.sh    # MongoDB tests
    ├── test-s3.sh         # S3 tests (LocalStack)
    ├── test-sqs.sh        # SQS tests (LocalStack)
    └── test-kafka.sh      # Kafka tests
```

## Test Commands

```bash
# Run all tests
make test

# Run specific test suite
make test-postgres
make test-mysql
make test-mongodb
make test-s3
make test-sqs
make test-kafka
```

## Test Data

All services are initialized with sample data:

- **PostgreSQL/MySQL**: `test_data`, `users`, `departments` tables (5 rows each)
- **MongoDB**: `users`, `orders`, `test_data` collections (5 documents each)
- **S3**: `dataql-test-bucket` with CSV, JSON, JSONL fixtures
- **SQS**: `dataql-test-queue` with sample JSON messages
- **DynamoDB**: `dataql-test-table` with sample items
- **Kafka**: `dataql-test-topic` with JSON messages

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

- `DATAQL_TEST_POSTGRES_URL` - PostgreSQL connection
- `DATAQL_TEST_MYSQL_URL` - MySQL connection
- `DATAQL_TEST_MONGODB_URL` - MongoDB connection
- `DATAQL_TEST_KAFKA_URL` - Kafka connection
- `DATAQL_TEST_S3_CSV` - S3 CSV file
- `DATAQL_TEST_S3_JSON` - S3 JSON file
- `DATAQL_TEST_S3_JSONL` - S3 JSONL file
- `DATAQL_TEST_SQS_URL` - SQS queue

## Troubleshooting

### Services not starting
```bash
make clean      # Remove old containers/volumes
make up         # Start fresh
```

### Check service logs
```bash
make logs                                    # All services
docker-compose logs -f postgres              # Specific service
```

### Verify test data
```bash
make verify-data
```

### Port conflicts
If ports conflict with existing services, edit `docker-compose.yaml` to use different external ports.
