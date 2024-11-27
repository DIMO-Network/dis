# dis

dis (DIMO Ingest Server) is a server that recieves data from data providers and stores the various data.

TODO add more info here about certs routing and cloud event conversion and signals conversion

## Provider Authentication

External providers must authenticate with the server using TLS client certificates. 
The server will verify the client certificate against the CA certificate root.
Based on the client certificate CN, the server will determine the provider and the provider's configuration.

## Provider Configuration

Each provider should be added to the `connections/` files(dev and prod).

**connectionID** is the CN of the client certificate that the provider will use to authenticate with the server and at the same time it is 0x address of the provider.

**chain_id** is the chain id of the provider. It is the same for all providers.

**aftermarket_contract_addr** and **vehicle_contract_addr** should be the same for all aftermarket devices.

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
