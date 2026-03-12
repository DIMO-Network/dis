//go:build integration

package integration

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttestationEndpoint(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:555"

	// Generate an Ethereum key pair for signing
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	ethAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Build attestation data
	attestationData := []map[string]any{
		{"subject": subject},
		{"insured": true},
		{"provider": "Test Insurance"},
		{"coverageStartDate": 1744751357},
		{"expirationDate": 1807822654},
	}
	dataBytes, err := json.Marshal(attestationData)
	require.NoError(t, err)

	// Sign the data with Ethereum text hash prefix
	msgHash := accounts.TextHash(dataBytes)
	sig, err := crypto.Sign(msgHash, privateKey)
	require.NoError(t, err)
	// Adjust v byte (Ethereum convention: 27 or 28)
	sig[64] += 27

	payload := map[string]any{
		"id":             "test-attestation-001",
		"subject":        subject,
		"time":           "2025-03-27T18:35:46.436008782Z",
		"vehicleTokenId": 555,
		"signature":      "0x" + common.Bytes2Hex(sig),
		"data":           attestationData,
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	// Send via JWT endpoint
	resp := postJWTAttestation(t, payloadBytes, ethAddr)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "attestation endpoint should return 200 for valid payload")

	// Wait for parquet batch flush (5s period + buffer)
	time.Sleep(8 * time.Second)

	// Check MinIO for parquet files
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	// Find the attestation event in parquet
	var found bool
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Type == "dimo.attestation" && ev.Subject == subject {
				assert.Equal(t, "1.0", ev.SpecVersion)
				assert.Equal(t, ethAddr.Hex(), ev.Source)
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	assert.True(t, found, "attestation CloudEvent not found in parquet files")

	// ── ClickHouse cloud_event table — 1 index row ───────────
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 1, "expected 1 cloud_event index row (dimo.attestation)")
	assert.Equal(t, "dimo.attestation", ceRows[0].EventType)
	assert.Equal(t, ethAddr.Hex(), ceRows[0].Source)
	assert.Equal(t, subject, ceRows[0].Subject)
}

func TestLargeAttestationDirectS3(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:556"

	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	ethAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Build a data payload large enough to exceed the 1024-byte threshold set in tests.
	largeData := strings.Repeat("x", 2048)
	attestationData := []map[string]any{
		{"subject": subject},
		{"imageData": largeData},
	}
	dataBytes, err := json.Marshal(attestationData)
	require.NoError(t, err)

	msgHash := accounts.TextHash(dataBytes)
	sig, err := crypto.Sign(msgHash, privateKey)
	require.NoError(t, err)
	sig[64] += 27

	payload := map[string]any{
		"id":             "test-large-attestation-001",
		"subject":        subject,
		"time":           "2025-03-27T18:35:46.436008782Z",
		"vehicleTokenId": 556,
		"signature":      "0x" + common.Bytes2Hex(sig),
		"data":           attestationData,
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Large payload size: %d bytes", len(payloadBytes))

	resp := postJWTAttestation(t, payloadBytes, ethAddr)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode)

	// Wait for flush
	time.Sleep(8 * time.Second)

	// Should find a single-*.json object, not a parquet
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no objects found in MinIO")

	var singleKey string
	for _, k := range keys {
		if strings.Contains(k, "single-") && strings.HasSuffix(k, ".json") {
			singleKey = k
			break
		}
	}
	require.NotEmpty(t, singleKey, "no single-*.json object found in MinIO; got keys: %v", keys)
	t.Logf("Found single-event object: %s", singleKey)

	// No parquet should contain the large event
	for _, k := range keys {
		if !strings.HasSuffix(k, ".parquet") {
			continue
		}
		events := readParquetFromMinIO(t, k)
		for _, ev := range events {
			assert.NotEqual(t, subject, ev.Subject, "large event should not appear in parquet %s", k)
		}
	}

	// ClickHouse index row must point to the single-event object
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 1, "expected 1 cloud_event index row for large attestation")
	assert.Equal(t, "dimo.attestation", ceRows[0].EventType)
	assert.Equal(t, ethAddr.Hex(), ceRows[0].Source)
	assert.Equal(t, singleKey, ceRows[0].IndexKey, "index_key should point to the single-event object")
}
