//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	assert.Equal(t, 200, resp.StatusCode)

	time.Sleep(3 * time.Second)

	// Check Kafka signals topic — find our message by subject
	msgs := consumeKafka(t, "topic.device.signals", 10*time.Second)
	var found bool
	for _, msg := range msgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			found = true
			assert.Equal(t, "1.0", ce.SpecVersion)
			assert.Equal(t, "dimo.status", ce.Type)
			assert.Equal(t, testSourceAddress, ce.Source)
			assert.Equal(t, subject, ce.Producer)
			require.Len(t, ce.Data.Signals, 1)
			assert.Equal(t, "powertrainCombustionEngineECT", ce.Data.Signals[0].Name)
			assert.InDelta(t, 107.0, ce.Data.Signals[0].ValueNumber, 0.01)
			break
		}
	}
	assert.True(t, found, "signal CloudEvent not found in Kafka messages")

	// Check ClickHouse — exactly 1 signal row for this subject
	rows := querySignals(t, subject)
	require.Len(t, rows, 1, "expected exactly 1 signal row in ClickHouse for 1 input signal")
	assert.Equal(t, "powertrainCombustionEngineECT", rows[0].Name)
	assert.Equal(t, testSourceAddress, rows[0].Source)
	assert.Equal(t, subject, rows[0].Producer)
	assert.InDelta(t, 107.0, rows[0].ValueNumber, 0.01)
	assert.NotEmpty(t, rows[0].CloudEventID, "cloud_event_id should be set")
}
