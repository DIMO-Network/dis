//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func writeDISConfig() error {
	// Write config.yaml
	configYAML := fmt.Sprintf(`
http:
  address: 0.0.0.0:%d

logger:
  level: DEBUG
  format: logfmt

metrics:
  prometheus: {}

shutdown_timeout: 5s
`, disMetricsPort)

	if err := os.WriteFile(filepath.Join(tmpDir, "config.yaml"), []byte(configYAML), 0o644); err != nil {
		return err
	}

	// Write resources.yaml — noop processors like the Benthos test mocks
	resourcesYAML := `
rate_limit_resources:
  - label: "connection_rate_limit"
    local:
      count: 100000
      interval: 1s

processor_resources:
  - label: "dimo_error_count"
    noop: {}
  - label: "dimo_bad_request_sync_response"
    noop: {}
  - label: "dimo_internal_error_sync_response"
    noop: {}
  - label: "dimo_provider_input_count"
    noop: {}
  - label: "handle_db_connection_error"
    noop: {}
  - label: "handle_db_error"
    noop: {}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "resources.yaml"), []byte(resourcesYAML), 0o644); err != nil {
		return err
	}

	// Create streams dir and copy the real pipeline config
	streamsDir := filepath.Join(tmpDir, "streams")
	if err := os.MkdirAll(streamsDir, 0o755); err != nil {
		return err
	}

	streamFiles := []string{
		"external-ingest.yaml",
		"output-parquet.yaml",
		"output-clickhouse.yaml",
		"output-kafka.yaml",
	}
	for _, name := range streamFiles {
		srcPath, err := filepath.Abs("../../charts/dis/files/streams/" + name)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("read pipeline config %s: %w (path: %s)", name, err, srcPath)
		}
		if err := os.WriteFile(filepath.Join(streamsDir, name), data, 0o644); err != nil {
			return err
		}
	}

	// Patch output-parquet.yaml to point aws_s3 at local MinIO
	parquetCfgPath := filepath.Join(streamsDir, "output-parquet.yaml")
	parquetCfg, err := os.ReadFile(parquetCfgPath)
	if err != nil {
		return fmt.Errorf("read parquet config for patching: %w", err)
	}
	patched := strings.Replace(string(parquetCfg),
		`                        aws_s3:
                          bucket: "${PARQUET_BUCKET}"
                          region: "${S3_AWS_REGION}"`,
		`                        aws_s3:
                          bucket: "${PARQUET_BUCKET}"
                          region: "${S3_AWS_REGION}"
                          endpoint: "${S3_ENDPOINT}"
                          force_path_style_urls: ${S3_FORCE_PATH_STYLE}`,
		1)
	if err := os.WriteFile(parquetCfgPath, []byte(patched), 0o644); err != nil {
		return fmt.Errorf("write patched parquet config: %w", err)
	}

	// Patch all batch periods to 1s so integration tests don't wait for long flushes
	periodRe := regexp.MustCompile(`(period:\s*)"?\d+s"?`)
	for _, name := range streamFiles {
		path := filepath.Join(streamsDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		patched := periodRe.ReplaceAllString(string(data), `${1}"250ms"`)
		if patched != string(data) {
			if err := os.WriteFile(path, []byte(patched), 0o644); err != nil {
				return fmt.Errorf("patch batch periods in %s: %w", name, err)
			}
		}
	}

	// Create buffer directories
	for _, dir := range []string{"signals", "events", "parquet", "kafka"} {
		if err := os.MkdirAll(filepath.Join(tmpDir, "buffer", dir), 0o755); err != nil {
			return err
		}
	}

	return nil
}

func startDIS() error {
	binPath, err := filepath.Abs("../../bin/dis")
	if err != nil {
		return err
	}

	disCmd = exec.Command(binPath,
		"--chilled",
		"-r", filepath.Join(tmpDir, "resources.yaml"),
		"-c", filepath.Join(tmpDir, "config.yaml"),
		"streams", filepath.Join(tmpDir, "streams"),
	)

	// Set environment variables for the pipeline config
	disCmd.Env = append(os.Environ(),
		// Ports
		fmt.Sprintf("DIS_CONNECTION_ADDRESS=0.0.0.0:%d", disConnectionPort),
		fmt.Sprintf("DIS_ATTESTATION_ADDRESS=0.0.0.0:%d", disAttestationPort),

		// TLS
		fmt.Sprintf("TLS_CA_CERT_FILE=%s", tlsCACertPath),
		fmt.Sprintf("TLS_CERT_FILE=%s", tlsServerCertPath),
		fmt.Sprintf("TLS_KEY_FILE=%s", tlsServerKeyPath),

		// Kafka
		"KAFKA_BOOTSTRAP_SERVERS=localhost",
		fmt.Sprintf("KAFKA_BOOTSTRAP_PORT=%d", 19092),
		"KAFKA_SIGNALS_TOPIC=topic.device.signals",
		"KAFKA_EVENTS_TOPIC=topic.device.events",

		// ClickHouse — DIS uses individual vars for migrations AND DSN for pipeline
		fmt.Sprintf("CLICKHOUSE_DSN=%s", clickhouseDSN),
		"CLICKHOUSE_HOST=localhost",
		"CLICKHOUSE_PORT=19000",
		"CLICKHOUSE_USER=default",
		"CLICKHOUSE_PASSWORD=",
		"CLICKHOUSE_DATABASE=dimo",
		"CLICKHOUSE_DIMO_DATABASE=dimo",
		"CLICKHOUSE_INDEX_DATABASE=dimo_index",
		"CLICKHOUSE_SECURE=false",

		// S3 / MinIO
		fmt.Sprintf("PARQUET_BUCKET=%s", minioBucket),
		"S3_AWS_REGION=us-east-1",
		fmt.Sprintf("S3_AWS_ACCESS_KEY_ID=%s", minioAccessKey),
		fmt.Sprintf("S3_AWS_SECRET_ACCESS_KEY=%s", minioSecretKey),
		fmt.Sprintf("S3_ENDPOINT=http://localhost:%d", 19090),
		"S3_FORCE_PATH_STYLE=true",
		"LARGE_EVENT_THRESHOLD=1048576",

		// Buffer
		fmt.Sprintf("DIS_BUFFER_DIR=%s/buffer", tmpDir),

		// RPC
		fmt.Sprintf("RPC_URL=%s", rpcServer.URL),

		// Chain / NFT addresses
		"DIMO_REGISTRY_CHAIN_ID=137",
		"VEHICLE_NFT_ADDRESS=0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF",
		"AFTERMARKET_NFT_ADDRESS=0x9c94C395cBcBDe662235E0A9d3bB87Ad708561BA",
		"SYNTHETIC_NFT_ADDRESS=0x4804e8D1661cd1a1e5dDdE1ff458A7f878c0aC6D",

		// JWT / Auth
		fmt.Sprintf("TOKEN_EXCHANGE_ISSUER=%s", jwtIssuerURL),
		fmt.Sprintf("TOKEN_EXCHANGE_KEY_SET_URL=%s/keys", jwtIssuerURL),

		// Log level
		"LOG_LEVEL=DEBUG",
	)

	disCmd.Stdout = os.Stdout
	disCmd.Stderr = os.Stderr

	return disCmd.Start()
}

func stopDIS() {
	if disCmd != nil && disCmd.Process != nil {
		disCmd.Process.Kill()
		disCmd.Wait()
	}
}

func waitForDIS() error {
	deadline := time.Now().Add(30 * time.Second)
	healthURL := fmt.Sprintf("http://localhost:%d/ping", disMetricsPort)

	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("DIS did not become ready within 30s")
}
