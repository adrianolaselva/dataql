# Contributing to DataQL

Thank you for your interest in contributing to DataQL! This document provides guidelines and best practices for contributing to the project.

## Development Workflow

### Prerequisites

- Go 1.21 or later
- Docker and Docker Compose (for e2e tests)
- Make (for build automation)

### Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/adrianolaselva/dataql.git
   cd dataql
   ```

2. Install dependencies:
   ```bash
   make mod-download
   ```

3. Build the project:
   ```bash
   make build
   ```

### Running Tests

#### Unit Tests

Run unit tests with:
```bash
make test
```

For test coverage:
```bash
make coverage
```

#### E2E Tests (Integration Tests)

E2E tests are **critical** for ensuring DataQL works correctly with all supported data sources. They must be run locally before submitting any PR.

**Starting the E2E Environment:**
```bash
# Start all infrastructure (PostgreSQL, MySQL, MongoDB, Kafka, etc.)
make e2e-up

# Wait for services to be healthy
make e2e-wait
```

**Running E2E Tests:**
```bash
# Run all e2e tests
make e2e-test

# Or run from the e2e directory
cd e2e
make test
```

**Running Specific Test Suites:**
```bash
cd e2e
make test-postgres   # PostgreSQL tests
make test-mysql      # MySQL tests
make test-mongodb    # MongoDB tests
make test-kafka      # Kafka tests
```

**Stopping the E2E Environment:**
```bash
make e2e-down
```

## E2E Testing Requirements

### When to Run E2E Tests

**E2E tests MUST be run locally:**

1. **Before submitting any Pull Request** - All tests must pass
2. **After implementing a new feature** - Add tests for the new functionality
3. **After fixing a bug** - Verify the fix doesn't break existing functionality
4. **After modifying query processing** - Ensure all data sources work correctly
5. **After updating dependencies** - Verify compatibility

### Adding New E2E Tests

When adding new functionality, create corresponding e2e tests:

1. Add test cases to the appropriate `e2e/tests/test-*.sh` file
2. Follow the existing test structure:
   ```bash
   test_your_new_feature() {
       log_info "Test: Description of what you're testing"
       result=$($DATAQL_BIN run -q "YOUR QUERY" -f "$URL" 2>&1)
       if echo "$result" | grep -q "expected_output"; then
           log_pass "Your test description"
       else
           log_fail "Your test description" "$result"
       fi
   }
   ```
3. Add the test function call in the appropriate section
4. Run the full test suite to ensure no regressions

### E2E Test Structure

```
e2e/
├── docker-compose.yaml    # Infrastructure configuration
├── .env                   # Environment variables
├── Makefile               # Test commands
├── README.md              # E2E documentation
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
    ├── test-kafka.sh      # Kafka tests
    ├── test-s3.sh         # S3 tests (LocalStack)
    └── test-sqs.sh        # SQS tests (LocalStack)
```

## Pull Request Process

### Before Submitting

1. **Run linter:**
   ```bash
   make lint
   ```

2. **Run unit tests:**
   ```bash
   make test
   ```

3. **Run E2E tests:**
   ```bash
   make e2e-up
   make e2e-test
   make e2e-down
   ```

4. **Update documentation** if needed

### PR Requirements

- All CI checks must pass
- E2E tests must have been run locally (include confirmation in PR description)
- Code follows existing patterns and style
- New features have corresponding tests
- Commit messages are clear and descriptive

### PR Description Template

```markdown
## Summary
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## E2E Testing
- [ ] I have run `make e2e-test` locally
- [ ] All e2e tests pass
- [ ] New tests added for new functionality (if applicable)

## Test Results
<paste summary of test results here>
```

## Code Style

- Follow standard Go conventions
- Use `gofmt` for formatting
- Run `golangci-lint` before committing
- Keep functions focused and well-documented
- Write clear commit messages

## Reporting Issues

When reporting bugs, please include:

1. DataQL version (`dataql --version`)
2. Go version (`go version`)
3. Operating system
4. Steps to reproduce
5. Expected vs actual behavior
6. Relevant error messages

## Questions?

If you have questions about contributing, please open an issue with the "question" label.

---

Thank you for contributing to DataQL!
