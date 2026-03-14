//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMalformedJSON(t *testing.T) {
	payload := []byte("{not json")

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payload)
	drainAndClose(t, resp)

	time.Sleep(750 * time.Millisecond)

	// Verify no messages leaked to Kafka
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 5*time.Second)
	require.Empty(t, msgs, "malformed JSON should not produce any signal")
}

func TestUnsupportedCloudEventType(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:666"

	payload := map[string]any{
		"id":             "test-unsupported-type",
		"source":         "test-source",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.unknown",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 666,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals":   []map[string]any{},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)

	time.Sleep(750 * time.Millisecond)

	// Verify no signals appeared for this subject
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 5*time.Second)
	require.Empty(t, msgs, "unsupported CloudEvent type should not produce signals")
}

func TestEmptyPayload(t *testing.T) {
	payload := []byte{}

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payload)
	drainAndClose(t, resp)

	time.Sleep(750 * time.Millisecond)

	// Verify no messages leaked to Kafka
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 5*time.Second)
	require.Empty(t, msgs, "empty payload should not produce any signal")
}

func TestInvalidJWT(t *testing.T) {
	url := fmt.Sprintf("http://localhost:%d/", disAttestationPort)
	req, err := http.NewRequest("POST", url, bytes.NewReader([]byte(`{"test":"data"}`)))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer invalid-token-here")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	drainAndClose(t, resp)

	require.NotEqual(t, 200, resp.StatusCode, "invalid JWT should not return 200")
}

func TestMissingJWT(t *testing.T) {
	url := fmt.Sprintf("http://localhost:%d/", disAttestationPort)
	req, err := http.NewRequest("POST", url, bytes.NewReader([]byte(`{"test":"data"}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	// No Authorization header

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	drainAndClose(t, resp)

	require.NotEqual(t, 200, resp.StatusCode, "missing JWT should not return 200")
}
