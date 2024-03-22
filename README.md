# benthos-plugin

Benthos instance that includes DIMO specific processors.

## Build

```shell
make build
make docker
```

## Test

a docker-compose file is included to help with setting up a local test environment that includes a Kafka broker, Zookeeper, Clickhouse and the benthos-plugin instance.

```shell
docker-compose up -d
```
