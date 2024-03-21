# benthos-plugin
Benthos plugin for DIMO specific pipelines

## Build

```sh
make build
```
or
```sh
go build -o benthos-plugin
```

## Run

### To generate test config file
```sh
./benthos-plugin create -s stdin/check_signature/stdout > test.yaml
```

### To run the plugin
```sh
./benthos-plugin -c test.yaml
```

## Testing your changes
1. Update unit tests
2. Run `make test` to run the tests
3. Run `make lint` to run the linter
4. Run `make build` to build the binary
5. Run `./benthos-plugin create -s stdin/check_signature/stdout > test.yaml` to generate test config file
6. Run `./benthos-plugin -c test.yaml` to run the plugin
7. Supply the plugin with a test message and check the output:
```sh
echo '{"data": {"timestamp":1709656316768}}' | ./benthos-plugin -c test.yaml
```

> [!CAUTION]
> 🚧 This Repo is still under construction watchout for failing code 🚧 