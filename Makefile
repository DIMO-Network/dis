.PHONY: clean run build install dep test lint format
BIN_NAME			?= stream-vss
DEFAULT_INSTALL_DIR			:= $(go env GOPATH)/bin
DEFAULT_ARCH				:= $(shell go env GOARCH)
DEFAULT_GOOS				:= $(shell go env GOOS)
ARCH						?= $(DEFAULT_ARCH)
GOOS						?= $(DEFAULT_GOOS)
INSTALL_DIR					?= $(DEFAULT_INSTALL_DIR)
.DEFAULT_GOAL := run

build:
	@CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(ARCH) \
		go build -o bin/$(BIN_NAME) ./main.go

run: build
	@./bin/$(BIN_NAME)
all: clean target

clean:
	@rm -rf bin
	

install: build
	@install -d $(INSTALL_DIR)
	@rm -f $(INSTALL_DIR)/benthos
	@cp bin/* $(INSTALL_DIR)/


dep: 
	@go mod tidy

test:
	@go test ./...

lint:
	@golangci-lint run

format:
	@golangci-lint run --fix
