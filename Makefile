
.PHONY: clean run build install dep test lint format docker test-benthos tools-golangci-lint config-gen generate

PATHINSTBIN = $(abspath ./bin)
export PATH := $(PATHINSTBIN):$(PATH)
SHELL := env PATH=$(PATH) $(SHELL)

BIN_NAME					?= dis
DEFAULT_INSTALL_DIR			:= $(go env GOPATH)/bin
DEFAULT_ARCH				:= $(shell go env GOARCH)
DEFAULT_GOOS				:= $(shell go env GOOS)
ARCH						?= $(DEFAULT_ARCH)
GOOS						?= $(DEFAULT_GOOS)
INSTALL_DIR					?= $(DEFAULT_INSTALL_DIR)
.DEFAULT_GOAL := run

VERSION   := $(shell git describe --tags 2>/dev/null || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)

# List of supported GOOS and GOARCH
GOOS_LIST := linux darwin
GOARCH_LIST := amd64 arm64

# Dependency versions
GOLANGCI_VERSION   = latest

help:
	@echo "\nSpecify a subcommand:\n"
	@grep -hE '^[0-9a-zA-Z_-]+:.*?## .*$$' ${MAKEFILE_LIST} | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[0;36m%-20s\033[m %s\n", $$1, $$2}'
	@echo ""

build:
	@CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(ARCH) \
		go build -o bin/$(BIN_NAME) ./

build-all:## Build target for all supported GOOS and GOARCH
	@for goos in $(GOOS_LIST); do \
		for goarch in $(GOARCH_LIST); do \
			echo "Building for $$goos/$$goarch..."; \
			CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch \
			go build -o bin/$(BIN_NAME)-$$goos-$$goarch ./; \
		done \
	done

clean:
	@rm -rf bin

install: build
	@install -d $(INSTALL_DIR)
	@rm -f $(INSTALL_DIR)/benthos
	@cp bin/* $(INSTALL_DIR)/

dep: 
	@go mod tidy

test: test-benthos ## Run all tests
	@go test ./...
	
test-benthos: build ## Run Benthos tests
	dis test --log debug ./test-benthos/...


lint-benthos: build  ## Run Benthos linter
	@CLICKHOUSE_HOST="" CLICKHOUSE_PORT="" CLICKHOUSE_SIGNAL_DATABASE="" CLICKHOUSE_INDEX_DATABASE=""  CLICKHOUSE_USER="" CLICKHOUSE_PASSWORD="" \
		S3_AWS_ACCESS_KEY_ID="" S3_AWS_SECRET_ACCESS_KEY="" S3_CLOUDEVENT_BUCKET="" S3_CLOUDEVENT_TMP_BUCKET="" \
	dis lint -r ./charts/dis/files/resources.yaml ./charts/dis/files/config.yaml ./charts/dis/files/streams_dev/*

	@CLICKHOUSE_HOST="" CLICKHOUSE_PORT="" CLICKHOUSE_SIGNAL_DATABASE="" CLICKHOUSE_INDEX_DATABASE=""  CLICKHOUSE_USER="" CLICKHOUSE_PASSWORD="" \
	S3_AWS_ACCESS_KEY_ID="" S3_AWS_SECRET_ACCESS_KEY="" S3_CLOUDEVENT_BUCKET="" S3_CLOUDEVENT_TMP_BUCKET="" \
	dis lint -r ./charts/dis/files/resources.yaml ./charts/dis/files/config.yaml ./charts/dis/files/streams_prod/*

lint: lint-benthos ## Run linter for benthos config and go code
	golangci-lint version
	@golangci-lint run --timeout=30m

format:
	@golangci-lint run --fix

docker: dep
	@docker build -f ./docker/dockerfile . -t dimozone/$(BIN_NAME):$(VER_CUT)
	@docker tag dimozone/$(BIN_NAME):$(VER_CUT) dimozone/$(BIN_NAME):latest

tools-golangci-lint:
	@mkdir -p $(PATHINSTBIN)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(PATHINSTBIN) $(GOLANGCI_VERSION)

tools: tools-golangci-lint ## Install all tools


config-gen: ## Generate Benthos config files
	@go run ./cmd/config-gen -input_prod=./connections/connections_prod.yaml -input_dev=./connections/connections_dev.yaml -output_prod=charts/$(BIN_NAME)/files/streams_prod -output_dev=charts/$(BIN_NAME)/files/streams_dev 

generate: config-gen ## Run all generate commands
	@go generate ./...