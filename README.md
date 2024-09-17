# DIS

Benthos instance that includes DIMO specific processors.

## Build

```shell
make build
make docker
```

## Test

a docker-compose file is included to help with setting up a local test environment that includes a Kafka broker, Zookeeper, Clickhouse and the DIS instance.

```shell
docker-compose up -d
```

## Run

### To generate test config file

```sh
./DIS create -s stdin/check_signature/stdout > test.yaml
```

### To run the plugin

```sh
./DIS -c test.yaml
```

## Testing your changes

1. Update unit tests
2. Run `make test` to run the tests
3. Run `make lint` to run the linter
4. Run `make build` to build the binary
5. Run `./DIS create -s stdin/check_signature/stdout > test.yaml` to generate test config file
6. Run `./DIS -c test.yaml` to run the plugin
7. Supply the plugin with a test message and check the output:

```sh
echo '{"data": {"timestamp":1709656316768}}' | ./DIS -c test.yaml
```
