PROJECT_NAME=dataql
PROJECT_VENDOR=adrianolaselva
VERSION=latest

ifndef release
override release = $(VERSION)
endif

.PHONY: all build test lint run tidy mod-download deps build-linux docker-build clean coverage

all:
	git rev-parse HEAD

build:
	go build -a -ldflags="-s -w" -o $(PROJECT_NAME) -v ./cmd/main.go

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
	go run ./cmd/main.go

tidy:
	go mod tidy

mod-download:
	go mod download

deps:
	go get -d -v ./...

build-linux:
	GOOS=linux GOARCH=amd64 go build -a -ldflags="-s -w" -o $(PROJECT_NAME) -v ./cmd/main.go

build-darwin:
	GOOS=darwin GOARCH=amd64 go build -a -ldflags="-s -w" -o $(PROJECT_NAME) -v ./cmd/main.go

build-windows:
	GOOS=windows GOARCH=amd64 go build -a -ldflags="-s -w" -o $(PROJECT_NAME).exe -v ./cmd/main.go

docker-build:
	docker build --rm -f "Dockerfile" -t "$(PROJECT_VENDOR)/$(PROJECT_NAME):$(release)" "." --build-arg VERSION=$(release)

clean:
	rm -rf $(PROJECT_NAME) $(PROJECT_NAME).exe .tmp/
