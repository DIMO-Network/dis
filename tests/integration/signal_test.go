//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultModuleSignals(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:999"

	payload := map[string]any{
		"id":             "test-signal-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 999,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals": []map[string]any{
				{
					"timestamp": "2024-04-18T17:20:26.633Z",
					"name":      "powertrainCombustionEngineECT",
					"value":     107,
				},
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode)

	time.Sleep(750 * time.Millisecond)

	// Check Kafka signals topic
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.Len(t, msgs, 1, "expected exactly 1 signal message")
	ce := parseSignalCE(t, msgs[0])
	require.Equal(t, subject, ce.Subject)
	require.Equal(t, "1.0", ce.SpecVersion)
	require.Equal(t, "dimo.signals", ce.Type)
	require.Equal(t, testSourceAddress, ce.Source)
	require.Equal(t, subject, ce.Producer)
	require.Len(t, ce.Data.Signals, 1)
	require.Equal(t, "powertrainCombustionEngineECT", ce.Data.Signals[0].Name)
	require.InDelta(t, 107.0, ce.Data.Signals[0].ValueNumber, 0.01)

	// Check ClickHouse — exactly 1 signal row for this subject
	rows := querySignals(t, subject)
	require.Len(t, rows, 1, "expected exactly 1 signal row in ClickHouse for 1 input signal")
	require.Equal(t, "powertrainCombustionEngineECT", rows[0].Name)
	require.Equal(t, testSourceAddress, rows[0].Source)
	require.Equal(t, subject, rows[0].Producer)
	require.InDelta(t, 107.0, rows[0].ValueNumber, 0.01)
	require.NotEmpty(t, rows[0].CloudEventID, "cloud_event_id should be set")
}
