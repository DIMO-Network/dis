//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"crypto/rsa"
	"net/http/httptest"
)

// Global test state set up by TestMain
var (
	// DIS subprocess
	disCmd *exec.Cmd

	// TLS
	tlsCACertPath     string
	tlsServerCertPath string
	tlsServerKeyPath  string
	tlsClientCertPath string
	tlsClientKeyPath  string
	clientTLSConfig   *tls.Config

	// JWT
	jwtPrivateKey *rsa.PrivateKey
	jwtIssuerURL  string
	jwtKeyID      string

	// Clients
	kafkaClient      *kgo.Client
	kafkaAdminClient *kadm.Client
	clickhouseDB     *sql.DB
	minioClient      *minio.Client

	// Config
	tmpDir string

	// Ports — DIS listens on these
	disConnectionPort  = 29443
	disAttestationPort = 29442
	disMetricsPort     = 28888

	// Infra ports — from docker-compose
	kafkaAddr      = "localhost:19092"
	clickhouseDSN  = "clickhouse://localhost:19000/dimo"
	minioEndpoint  = "localhost:19090"
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	minioBucket    = "dimo-test-parquet"

	// Mock servers
	rpcServer  *httptest.Server
	jwksServer *httptest.Server

	// Source address used for mTLS CN (simulates a registered device source)
	testSourceAddress = "0xTestSourceAddr1234567890abcdef1234"

	// Ruptela source address — must match modules.RuptelaSource
	ruptelaSourceAddress = "0xF26421509Efe92861a587482100c6d728aBf1CD0"
	ruptelaTLSConfig     *tls.Config
)

func TestMain(m *testing.M) {
	var exitCode int
	defer func() { os.Exit(exitCode) }()

	ctx := context.Background()
	var err error

	// 1. Create temp dir for certs and config
	tmpDir, err = os.MkdirTemp("", "dis-integration-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		exitCode = 1
		return
	}
	defer os.RemoveAll(tmpDir)

	// 2. Generate TLS certs
	if err := generateCerts(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate certs: %v\n", err)
		exitCode = 1
		return
	}

	// 3. Start mock JWKS server
	startJWKSServer()
	defer jwksServer.Close()

	// 4. Start mock RPC server
	startRPCMock()
	defer rpcServer.Close()

	// 5. Setup ClickHouse (run migrations)
	if err := setupClickHouse(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup clickhouse: %v\n", err)
		exitCode = 1
		return
	}
	defer clickhouseDB.Close()

	// 6. Setup Kafka (create topics)
	if err := setupKafka(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup kafka: %v\n", err)
		exitCode = 1
		return
	}
	defer kafkaClient.Close()

	// 7. Setup MinIO (create bucket)
	if err := setupMinIO(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup minio: %v\n", err)
		exitCode = 1
		return
	}

	// 8. Write DIS config
	if err := writeDISConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write DIS config: %v\n", err)
		exitCode = 1
		return
	}

	// 9. Start DIS subprocess
	if err := startDIS(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start DIS: %v\n", err)
		exitCode = 1
		return
	}
	defer stopDIS()

	// 10. Wait for DIS to be ready
	if err := waitForDIS(); err != nil {
		fmt.Fprintf(os.Stderr, "DIS failed to become ready: %v\n", err)
		exitCode = 1
		return
	}

	// 11. Run tests
	exitCode = m.Run()

	_ = err // suppress unused warning
}
