//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuptelaFullPipeline(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:444"

	// Ruptela status payload with both signals and event counters.
	// Signal 96 = powertrainCombustionEngineECT (hex 0x6B=107, 107-40=67°C)
	// Signal 97 = exteriorAirTemperature (hex 0x3C=60, 60-40=20°C)
	// Note: 0xFF is "not available" for 1-byte sensors — must use valid values.
	// Signal 135 = braking events (0x35 = harsh:5, extreme:3)
	// Signal 136 = acceleration events (0x0A = 10)
	// Signal 143 = cornering events (0x07 = 7)
	// Signal 104-106 = VIN hex segments
	payload := map[string]any{
		"ds":             "r/v0/s",
		"signature":      "0xdeadbeef",
		"subject":        subject,
		"vehicleTokenId": 444,
		"deviceTokenId":  10,
		"time":           "2024-09-27T08:33:26Z",
		"data": map[string]any{
			"signals": map[string]string{
				"96":  "6B",               // powertrainCombustionEngineECT: 67°C
				"97":  "3C",               // exteriorAirTemperature: 20°C
				"135": "35",               // braking events
				"136": "A",                // acceleration events
				"143": "7",                // cornering events
				"104": "4148544241334344", // VIN part 1
				"105": "3930363235323539", // VIN part 2
				"106": "3300000000000000", // VIN part 3
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	// Use Ruptela mTLS client cert (CN = Ruptela source address)
	resp := postMTLSWithConfig(t, payloadBytes, ruptelaTLSConfig)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "DIS should accept valid Ruptela payload")

	// Wait for pipeline processing
	time.Sleep(5 * time.Second)

	// ── 1. Kafka signals topic ──────────────────────────────────
	signalMsgs := consumeKafka(t, "topic.device.signals", 10*time.Second)
	var signalFound bool
	for _, msg := range signalMsgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			signalFound = true
			assert.Equal(t, "1.0", ce.SpecVersion)
			assert.Equal(t, "dimo.status", ce.Type)
			assert.Equal(t, ruptelaSourceAddress, ce.Source)
			assert.NotEmpty(t, ce.Data.Signals, "should have decoded signals")
			break
		}
	}
	assert.True(t, signalFound, "signal CloudEvent not found in Kafka signals topic")

	// ── 2. Kafka events topic ───────────────────────────────────
	eventMsgs := consumeKafka(t, "topic.device.events", 10*time.Second)
	var eventFound bool
	for _, msg := range eventMsgs {
		ce := parseEventCE(t, msg)
		if ce.Subject == subject {
			eventFound = true
			assert.Equal(t, "1.0", ce.SpecVersion)
			assert.Equal(t, "dimo.event", ce.Type)
			assert.Equal(t, ruptelaSourceAddress, ce.Source)
			assert.NotEmpty(t, ce.Data.Events, "should have decoded events")
			break
		}
	}
	assert.True(t, eventFound, "event CloudEvent not found in Kafka events topic")

	// ── 3. ClickHouse signal table ──────────────────────────────
	signalRows := querySignals(t, subject)
	require.NotEmpty(t, signalRows, "no signal rows in ClickHouse")
	var chSignalFound bool
	for _, r := range signalRows {
		if r.Name == "powertrainCombustionEngineECT" {
			chSignalFound = true
			assert.Equal(t, ruptelaSourceAddress, r.Source)
		}
	}
	assert.True(t, chSignalFound, "expected signal not found in ClickHouse")

	// ── 4. ClickHouse event table ───────────────────────────────
	eventRows := queryEvents(t, subject)
	require.NotEmpty(t, eventRows, "no event rows in ClickHouse")
	var chEventFound bool
	for _, r := range eventRows {
		if r.Name == "behavior.harshBraking" || r.Name == "behavior.harshAcceleration" || r.Name == "behavior.harshCornering" {
			chEventFound = true
			assert.Equal(t, ruptelaSourceAddress, r.Source)
		}
	}
	assert.True(t, chEventFound, "expected event not found in ClickHouse event table")

	// ── 5. S3 (MinIO) parquet file ──────────────────────────────
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
				assert.Equal(t, ruptelaSourceAddress, ev.Source)
				break
			}
		}
		if pqFound {
			break
		}
	}
	assert.True(t, pqFound, "CloudEvent not found in S3 parquet files")
}
