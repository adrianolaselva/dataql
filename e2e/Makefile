# DataQL E2E Testing Makefile
# Run from e2e directory: make [target]
# Or from project root: make -C e2e [target]

.PHONY: all up down status logs clean wait test test-all \
        test-postgres test-mysql test-mongodb test-s3 test-sqs test-kafka \
        build shell-postgres shell-mysql shell-mongodb shell-redis reset help

# Default target
all: help

# Build the dataql binary
build:
	@echo "Building dataql binary..."
	@cd .. && go build -a -ldflags="-s -w" -o dataql -v ./main.go
	@echo "Build complete!"

# Start e2e infrastructure
up:
	@echo "Starting E2E infrastructure..."
	@docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@$(MAKE) wait
	@echo "E2E infrastructure is ready!"

# Stop e2e infrastructure
down:
	@echo "Stopping E2E infrastructure..."
	@docker-compose down
	@echo "E2E infrastructure stopped."

# Check status
status:
	@echo "E2E Infrastructure Status:"
	@echo "=========================="
	@docker-compose ps

# View logs
logs:
	@docker-compose logs -f

# Remove volumes and clean up
clean:
	@echo "Cleaning E2E infrastructure..."
	@docker-compose down -v --remove-orphans
	@echo "E2E infrastructure cleaned."

# Wait for all services to be healthy
wait:
	@echo "Waiting for PostgreSQL..."
	@timeout 60 bash -c 'until docker exec dataql-postgres pg_isready -U dataql -d dataql_test > /dev/null 2>&1; do sleep 2; done' || (echo "PostgreSQL timeout" && exit 1)
	@echo "✓ PostgreSQL ready"
	@echo "Waiting for MySQL..."
	@timeout 60 bash -c 'until docker exec dataql-mysql mysqladmin ping -u dataql -pdataql_pass --silent > /dev/null 2>&1; do sleep 2; done' || (echo "MySQL timeout" && exit 1)
	@echo "✓ MySQL ready"
	@echo "Waiting for MongoDB..."
	@timeout 60 bash -c 'until docker exec dataql-mongodb mongosh --eval "db.runCommand({ping:1})" > /dev/null 2>&1; do sleep 2; done' || (echo "MongoDB timeout" && exit 1)
	@echo "✓ MongoDB ready"
	@echo "Waiting for Kafka..."
	@timeout 90 bash -c 'until docker exec dataql-kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do sleep 2; done' || (echo "Kafka timeout" && exit 1)
	@echo "✓ Kafka ready"
	@echo "Waiting for LocalStack..."
	@timeout 60 bash -c 'until curl -s http://localhost:24566/_localstack/health | grep -q "running"; do sleep 2; done' || (echo "LocalStack timeout" && exit 1)
	@echo "✓ LocalStack ready"
	@echo "Waiting for Redis..."
	@timeout 30 bash -c 'until docker exec dataql-redis redis-cli ping > /dev/null 2>&1; do sleep 2; done' || (echo "Redis timeout" && exit 1)
	@echo "✓ Redis ready"
	@echo ""
	@echo "All services are healthy!"

# Reset e2e environment (clean + up)
reset: clean up
	@echo "E2E environment has been reset!"

# ============================================
# Test Targets
# ============================================

# Run all tests
test: test-all

test-all: build
	@echo "Running all E2E tests..."
	@chmod +x tests/*.sh
	@./tests/test-all.sh

# Run PostgreSQL tests only
test-postgres: build
	@echo "Running PostgreSQL tests..."
	@chmod +x tests/test-postgres.sh
	@./tests/test-postgres.sh

# Run MySQL tests only
test-mysql: build
	@echo "Running MySQL tests..."
	@chmod +x tests/test-mysql.sh
	@./tests/test-mysql.sh

# Run MongoDB tests only
test-mongodb: build
	@echo "Running MongoDB tests..."
	@chmod +x tests/test-mongodb.sh
	@./tests/test-mongodb.sh

# Run S3 tests only
test-s3: build
	@echo "Running S3 tests..."
	@chmod +x tests/test-s3.sh
	@./tests/test-s3.sh

# Run SQS tests only
test-sqs: build
	@echo "Running SQS tests..."
	@chmod +x tests/test-sqs.sh
	@./tests/test-sqs.sh

# Run Kafka tests only
test-kafka: build
	@echo "Running Kafka tests..."
	@chmod +x tests/test-kafka.sh
	@./tests/test-kafka.sh

# ============================================
# Shell Access
# ============================================

shell-postgres:
	@docker exec -it dataql-postgres psql -U dataql -d dataql_test

shell-mysql:
	@docker exec -it dataql-mysql mysql -u dataql -pdataql_pass dataql_test

shell-mongodb:
	@docker exec -it dataql-mongodb mongosh dataql_test -u dataql -p dataql_pass --authenticationDatabase admin

shell-redis:
	@docker exec -it dataql-redis redis-cli

shell-kafka:
	@docker exec -it dataql-kafka bash

# ============================================
# Utilities
# ============================================

# Verify data in all services
verify-data:
	@echo "Verifying PostgreSQL data..."
	@docker exec dataql-postgres psql -U dataql -d dataql_test -c "SELECT COUNT(*) FROM test_data;"
	@echo ""
	@echo "Verifying MySQL data..."
	@docker exec dataql-mysql mysql -u dataql -pdataql_pass -e "SELECT COUNT(*) FROM test_data;" dataql_test
	@echo ""
	@echo "Verifying MongoDB data..."
	@docker exec dataql-mongodb mongosh --quiet dataql_test -u dataql -p dataql_pass --authenticationDatabase admin --eval "db.test_data.countDocuments()"
	@echo ""
	@echo "Verifying Kafka topics..."
	@docker exec dataql-kafka kafka-topics --list --bootstrap-server localhost:9092
	@echo ""
	@echo "Verifying LocalStack S3..."
	@AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:24566 s3 ls s3://dataql-test-bucket/fixtures/

# Full e2e workflow: build, up, test, down
full: build up test down
	@echo "Full E2E workflow completed!"

# Help
help:
	@echo "DataQL E2E Testing"
	@echo "=================="
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up          - Start all services"
	@echo "  make down        - Stop all services"
	@echo "  make status      - Show service status"
	@echo "  make logs        - Follow service logs"
	@echo "  make clean       - Stop and remove volumes"
	@echo "  make reset       - Clean and restart"
	@echo "  make wait        - Wait for services to be healthy"
	@echo ""
	@echo "Testing:"
	@echo "  make test        - Run all tests"
	@echo "  make test-all    - Run all tests"
	@echo "  make test-postgres - Run PostgreSQL tests only"
	@echo "  make test-mysql    - Run MySQL tests only"
	@echo "  make test-mongodb  - Run MongoDB tests only"
	@echo "  make test-s3       - Run S3 tests only"
	@echo "  make test-sqs      - Run SQS tests only"
	@echo "  make test-kafka    - Run Kafka tests only"
	@echo ""
	@echo "Shell Access:"
	@echo "  make shell-postgres - Open psql shell"
	@echo "  make shell-mysql    - Open mysql shell"
	@echo "  make shell-mongodb  - Open mongosh shell"
	@echo "  make shell-redis    - Open redis-cli shell"
	@echo "  make shell-kafka    - Open Kafka container bash"
	@echo ""
	@echo "Utilities:"
	@echo "  make build       - Build dataql binary"
	@echo "  make verify-data - Verify test data in all services"
	@echo "  make full        - Full workflow: build, up, test, down"
	@echo ""
