# DIS

DIS (DIMO Ingest Server) is a server that recieves data from data providers and stores the various data.

## Provider Authentication

External providers must authenticate with the server using TLS client certificates. 
The server will verify the client certificate against the CA certificate root.
Based on the client certificate CN, the server will determine the provider and the provider's configuration.

## Provider Configuration

Each provider should be added to the `connections/` files(dev and prod).
**connectionID** is the CN of the client certificate that the provider will use to authenticate with the server and at the same time it is 0x address of the provider.

## Generate Benthos config

After you have made changes to the provider configs(connections_dev.yaml and connections_prod.yaml), you will
need to run make generate to update the benthos config to contain your provider config changes.

```shell
make generate
```

## Build

```shell
make build
make docker
```

Use `make help` to see all options.
``` 
> make help
Specify a subcommand:

  build-all            Build target for all supported GOOS and GOARCH
  test                 Run all tests
  test-go              Run Go tests
  test-benthos         Run Benthos tests
  test-prometheus-prepare Prepare Prometheus alert files for testing
  test-prometheus-alerts Check Prometheus alert rules
  test-prometheus-rules Run Prometheus rules tests
  lint-benthos         Run Benthos linter
  lint                 Run linter for benthos config and go code
  tools-prometheus     Install Prometheus and promtool
  tools-mockgen        install mockgen tool
  tools                Install all tools
  config-gen           Generate Benthos config files
  generate             Run all generate commands
```
