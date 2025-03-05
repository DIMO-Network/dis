# DIMO INGEST Server (DIS)

DIS (DIMO Ingest Server) is a server that receives data from data providers and stores the various data.

## Getting Started

If you want to integrate your data with DIMO, you can get started quickly by posting data to DIS using our default data Format.

1. Mint a Connections License On Chain
2. Generate a TLS client certificate
3. Post Data to the DIS Server https://dis.dimo.zone using MTLS with the client certificate

## Data Format

When posting data to the DIS server, you must format your payload according to the following CloudEvent specification for proper processing and storage.

```json
{
  "id": "unique-event-identifier",
  "source": "0xConnectionLicenseAddress",
  "producer": "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_42",
  "specversion": "1.0",
  "subject": "did:nft:1:0x123456789abcdef0123456789abcdef012345678_123",
  "time": "2025-03-04T12:00:00Z",
  "type": "dimo.status",
  "datacontenttype": "application/json",
  "dataversion": "default/v1.0",
  "data": {
    "signals": [
      {
        "name": "powertrainTransmissionTravelledDistance",
        "timestamp": "2025-03-04T12:00:00Z",
        "value": 12345.67
      },
      {
        "name": "speed",
        "timestamp": "2025-03-04T12:01:00Z",
        "value": 55
      },
      {
        "name": "powertrainType",
        "timestamp": "2025-03-04T12:03:00Z",
        "value": "COMBUSTION"
      }
    ],
    "vin": "1GGCM82633A123456"
  }
}
```

### Cloud Event Header Descriptions

- **id**: A unique identifier for the event. The combination of ID and Source must be unique.
- **source**: The connection license address. Note that this field will be overwritten with the connection license address pulled from the CN of the certificate used for authentication.
- **producer**: The NFT DID of the paired device that produced the payload. Must follow the format `did:nft:<chainId>:<contractAddress>_<tokenId>`.
- **specversion**: The version of CloudEvents specification used. This is always hardcoded as "1.0".
- **subject**: The NFT DID which denotes which vehicle token ID is being used. Must follow the format `did:nft:<chainId>:<contractAddress>_<tokenId>`.
- **time**: The time at which the event occurred. In practice, we always set this. Format as ISO 8601 timestamp.
- **type**: Describes the type of event one of `dimo.status, dimo.fingerprint`
- **datacontenttype**: An optional MIME type for the data field. We almost always serialize to JSON and in that case this field is implicitly "application/json".
- **dataversion**: An optional way for the data provider to give more information about the type of data in the payload.

### Data Object Structure

> Note: If you don't want to use the default data format, you can submit a PR to add custom decoding for your own format. This allows for greater flexibility when integrating with existing systems.

The `data` field contains the actual vehicle data with the following structure:

1. **signals**: An array of signal objects representing vehicle data points. Each signal object has the following structure:

   ```json
   {
     "name": "vssName",
     "timestamp": "ISO-8601 timestamp",
     "value": 123.45
   }
   ```

   Note: A signal's value can either be a number or a string. If the value is string, a numeric value will not be accepted.

2. **vin**: A string representing the Vehicle Identification Number.
   ```json
   "vin": "1GGCM82633A123456"
   ```

### Event Type Processing

The server processes your data payload and determines how it will be stored:

- If the `signals` field is present in the data, a new cloud event will be stored with the type `status`.
- If the `vin` field is present in the data, it will be stored with the type `fingerprint`.
- If both are present, then two separate cloud events will be created and stored - one as a `status` payload and one as a `fingerprint`.

### NFT DID Format

The NFT DID (Decentralized Identifier) follows the format:

```
did:nft:<chainId>:<contractAddress>_<tokenId>
```

Example: `did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_42`

Where:

- `did:nft:` is the prefix that identifies this as an NFT DID
- `<chainId>` is the numeric ID of the blockchain (e.g., 137 for Polygon)
- `<contractAddress>` is the hex address of the NFT contract of the NFT
- `<tokenId>` is the numeric ID of the specific token

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
