//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCloudEventParquet(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")

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
	t.Logf("Input payload: %s", string(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode)

	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka signals topic — verify the signal CE was produced ──
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.Len(t, msgs, 1, "expected exactly 1 signal message")
	ce := parseSignalCE(t, msgs[0])
	require.Equal(t, subject, ce.Subject)
	require.Equal(t, "dimo.signals", ce.Type)
	require.Equal(t, testSourceAddress, ce.Source)
	require.Len(t, ce.Data.Signals, 1, "expected exactly 1 signal in Kafka CE")
	require.Equal(t, "speed", ce.Data.Signals[0].Name)
	require.InDelta(t, 55.0, ce.Data.Signals[0].ValueNumber, 0.01)

	// ── 2. ClickHouse — verify signal row was written ──────────────
	rows := querySignals(t, subject)
	require.Len(t, rows, 1, "expected exactly 1 signal row in ClickHouse")
	require.Equal(t, "speed", rows[0].Name)
	require.InDelta(t, 55.0, rows[0].ValueNumber, 0.01)
	require.Equal(t, testSourceAddress, rows[0].Source)

	// ── 3. MinIO parquet — verify CloudEvent was archived ──────────
	// Wait for parquet batch flush
	time.Sleep(750 * time.Millisecond)

	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqFound bool
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqFound = true
				require.Equal(t, testSourceAddress, ev.Source)
				require.Equal(t, "dimo.status", ev.Type)
				require.Equal(t, "1.0", ev.SpecVersion)
				require.Equal(t, subject, ev.Producer)
				break
			}
		}
		if pqFound {
			break
		}
	}
	require.True(t, pqFound, "expected CloudEvent not found in parquet file")
}
