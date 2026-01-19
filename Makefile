PROJECT_NAME=dataql
PROJECT_VENDOR=adrianolaselva
VERSION=latest
INSTALL_DIR=/usr/local/bin
LOCAL_INSTALL_DIR=$(HOME)/.local/bin

ifndef release
override release = $(VERSION)
endif

.PHONY: all build test lint run tidy mod-download deps clean coverage \
        build-linux build-linux-arm64 build-darwin build-darwin-arm64 build-windows \
        build-all install install-local uninstall release-dry-run docker-build \
        verify verify-binary \
        e2e-up e2e-down e2e-logs e2e-status e2e-clean e2e-test e2e-wait e2e-reset

all:
	git rev-parse HEAD

build:
	go build -a -ldflags="-s -w" -o $(PROJECT_NAME) -v ./main.go

test:
	go test -v -race -count=1 -short ./...

coverage:
	@mkdir -p .tmp
	go test -v -race -coverprofile=.tmp/coverage.out ./...
	go tool cover -html=.tmp/coverage.out -o .tmp/coverage.html

lint:
	golangci-lint run ./...

lint-out:
	@mkdir -p .tmp
	golangci-lint run --out-format checkstyle > .tmp/lint.out

run:
	go run ./main.go

tidy:
	go mod tidy

mod-download:
	go mod download

deps:
	go get -d -v ./...

# Platform-specific builds
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags="-s -w" -o dist/$(PROJECT_NAME)_linux_amd64 -v ./main.go

build-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -a -ldflags="-s -w" -o dist/$(PROJECT_NAME)_linux_arm64 -v ./main.go

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -a -ldflags="-s -w" -o dist/$(PROJECT_NAME)_darwin_amd64 -v ./main.go

build-darwin-arm64:
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -a -ldflags="-s -w" -o dist/$(PROJECT_NAME)_darwin_arm64 -v ./main.go

build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -a -ldflags="-s -w" -o dist/$(PROJECT_NAME)_windows_amd64.exe -v ./main.go

# Build for all platforms
build-all: clean
	@mkdir -p dist
	$(MAKE) build-linux
	$(MAKE) build-linux-arm64
	$(MAKE) build-darwin
	$(MAKE) build-darwin-arm64
	$(MAKE) build-windows
	@echo "All binaries built in dist/"

# Installation targets
install: build
	sudo cp $(PROJECT_NAME) $(INSTALL_DIR)/$(PROJECT_NAME)
	sudo chmod +x $(INSTALL_DIR)/$(PROJECT_NAME)
	@echo "Installed to $(INSTALL_DIR)/$(PROJECT_NAME)"

install-local: build
	@mkdir -p $(LOCAL_INSTALL_DIR)
	cp $(PROJECT_NAME) $(LOCAL_INSTALL_DIR)/$(PROJECT_NAME)
	chmod +x $(LOCAL_INSTALL_DIR)/$(PROJECT_NAME)
	@echo "Installed to $(LOCAL_INSTALL_DIR)/$(PROJECT_NAME)"
	@echo "Make sure $(LOCAL_INSTALL_DIR) is in your PATH"

uninstall:
	@if [ -f $(INSTALL_DIR)/$(PROJECT_NAME) ]; then \
		sudo rm -f $(INSTALL_DIR)/$(PROJECT_NAME); \
		echo "Removed $(INSTALL_DIR)/$(PROJECT_NAME)"; \
	fi
	@if [ -f $(LOCAL_INSTALL_DIR)/$(PROJECT_NAME) ]; then \
		rm -f $(LOCAL_INSTALL_DIR)/$(PROJECT_NAME); \
		echo "Removed $(LOCAL_INSTALL_DIR)/$(PROJECT_NAME)"; \
	fi

# Release (requires goreleaser)
release-dry-run:
	goreleaser release --snapshot --clean

docker-build:
	docker build --rm -f "Dockerfile" -t "$(PROJECT_VENDOR)/$(PROJECT_NAME):$(release)" "." --build-arg VERSION=$(release)

clean:
	rm -rf $(PROJECT_NAME) $(PROJECT_NAME).exe dist/ .tmp/

# Verify binary has all expected commands
verify-binary: build
	@echo "Verifying binary commands..."
	@./$(PROJECT_NAME) --version
	@./$(PROJECT_NAME) run --help > /dev/null && echo "✓ run command OK"
	@./$(PROJECT_NAME) skills --help > /dev/null && echo "✓ skills command OK"
	@./$(PROJECT_NAME) mcp --help > /dev/null && echo "✓ mcp command OK"
	@./$(PROJECT_NAME) mcp serve --help > /dev/null && echo "✓ mcp serve command OK"
	@./$(PROJECT_NAME) skills install --help > /dev/null && echo "✓ skills install command OK"
	@./$(PROJECT_NAME) skills list --help > /dev/null && echo "✓ skills list command OK"
	@echo "All commands verified successfully!"

# Verify installed binary
verify:
	@chmod +x scripts/verify-install.sh
	@scripts/verify-install.sh

# ============================================
# E2E Testing Infrastructure
# ============================================

E2E_DIR=e2e
E2E_COMPOSE=docker-compose -f $(E2E_DIR)/docker-compose.yaml -p dataql-e2e
E2E_ENV_FILE=$(E2E_DIR)/.env

# Start e2e infrastructure
e2e-up:
	@echo "Starting E2E infrastructure..."
	$(E2E_COMPOSE) up -d
	@echo "Waiting for services to be healthy..."
	@$(MAKE) e2e-wait
	@echo "E2E infrastructure is ready!"

# Stop e2e infrastructure
e2e-down:
	@echo "Stopping E2E infrastructure..."
	$(E2E_COMPOSE) down
	@echo "E2E infrastructure stopped."

# View logs
e2e-logs:
	$(E2E_COMPOSE) logs -f

# Check status
e2e-status:
	@echo "E2E Infrastructure Status:"
	@echo "=========================="
	$(E2E_COMPOSE) ps

# Remove volumes and clean up
e2e-clean:
	@echo "Cleaning E2E infrastructure..."
	$(E2E_COMPOSE) down -v --remove-orphans
	@echo "E2E infrastructure cleaned."

# Wait for all services to be healthy
e2e-wait:
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

# Run e2e tests
e2e-test: build
	@echo "Running E2E tests..."
	@echo ""
	@echo "=== Database Tests (PostgreSQL/MySQL) ==="
	@set -a && source $(E2E_ENV_FILE) && set +a && \
		go test -v -race -count=1 ./pkg/datasource/... -run "Integration|E2E" 2>/dev/null || true
	@echo ""
	@echo "=== MongoDB Tests ==="
	@set -a && source $(E2E_ENV_FILE) && set +a && \
		go test -v -race -count=1 ./pkg/mongodb/... -run "Integration|E2E" 2>/dev/null || true
	@echo ""
	@echo "=== S3 Tests ==="
	@set -a && source $(E2E_ENV_FILE) && set +a && \
		go test -v -race -count=1 ./pkg/filehandler/s3/... -run "Integration|E2E" 2>/dev/null || true
	@echo ""
	@echo "=== SQS Tests ==="
	@set -a && source $(E2E_ENV_FILE) && set +a && \
		go test -v -race -count=1 ./pkg/mqreader/sqs/... -run "Integration|E2E" 2>/dev/null || true
	@echo ""
	@echo "=== Kafka Tests ==="
	@set -a && source $(E2E_ENV_FILE) && set +a && \
		go test -v -race -count=1 ./pkg/mqreader/kafka/... -run "Integration|E2E" 2>/dev/null || true
	@echo ""
	@echo "=== All Unit Tests ==="
	go test -v -race -count=1 -short ./...
	@echo ""
	@echo "E2E tests completed!"

# Reset e2e environment (clean + up)
e2e-reset: e2e-clean e2e-up
	@echo "E2E environment has been reset!"

# Shell access to containers
e2e-shell-postgres:
	docker exec -it dataql-postgres psql -U dataql -d dataql_test

e2e-shell-mysql:
	docker exec -it dataql-mysql mysql -u dataql -pdataql_pass dataql_test

e2e-shell-mongodb:
	docker exec -it dataql-mongodb mongosh dataql_test -u dataql -p dataql_pass

e2e-shell-redis:
	docker exec -it dataql-redis redis-cli