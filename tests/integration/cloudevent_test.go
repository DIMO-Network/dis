//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudEventParquet(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:777"

	payload := map[string]any{
		"id":             "test-ce-parquet-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 777,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals": []map[string]any{
				{
					"timestamp": "2024-04-18T17:20:26.633Z",
					"name":      "speed",
					"value":     55,
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

	// ── 1. Kafka signals topic — verify the signal CE was produced ──
	msgs := consumeKafka(t, "topic.device.signals", 10*time.Second)
	var kafkaFound bool
	for _, msg := range msgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			kafkaFound = true
			assert.Equal(t, "dimo.status", ce.Type)
			assert.Equal(t, testSourceAddress, ce.Source)
			require.Len(t, ce.Data.Signals, 1, "expected exactly 1 signal in Kafka CE")
			assert.Equal(t, "speed", ce.Data.Signals[0].Name)
			assert.InDelta(t, 55.0, ce.Data.Signals[0].ValueNumber, 0.01)
			break
		}
	}
	assert.True(t, kafkaFound, "signal CloudEvent not found in Kafka signals topic")

	// ── 2. ClickHouse — verify signal row was written ──────────────
	rows := querySignals(t, subject)
	require.Len(t, rows, 1, "expected exactly 1 signal row in ClickHouse")
	assert.Equal(t, "speed", rows[0].Name)
	assert.InDelta(t, 55.0, rows[0].ValueNumber, 0.01)
	assert.Equal(t, testSourceAddress, rows[0].Source)

	// ── 3. MinIO parquet — verify CloudEvent was archived ──────────
	// Wait for parquet batch flush (5s period + buffer)
	time.Sleep(6 * time.Second)

	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqFound bool
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqFound = true
				assert.Equal(t, testSourceAddress, ev.Source)
				assert.Equal(t, "dimo.status", ev.Type)
				assert.Equal(t, "1.0", ev.SpecVersion)
				assert.Equal(t, subject, ev.Producer)
				break
			}
		}
		if pqFound {
			break
		}
	}
	assert.True(t, pqFound, "expected CloudEvent not found in parquet file")
}
