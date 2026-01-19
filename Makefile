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
        verify verify-binary

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
