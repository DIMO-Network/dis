//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
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
	var signalCE *vss.SignalCloudEvent
	for _, msg := range signalMsgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			signalCE = &ce
			break
		}
	}
	require.NotNil(t, signalCE, "signal CloudEvent not found in Kafka signals topic")
	assert.Equal(t, "1.0", signalCE.SpecVersion)
	assert.Equal(t, "dimo.status", signalCE.Type)
	assert.Equal(t, ruptelaSourceAddress, signalCE.Source)
	// Ruptela OIDs 96 (ECT) and 97 (exterior temp) should produce 2 signals
	// OIDs 104-106 (VIN) may produce additional signal(s)
	require.GreaterOrEqual(t, len(signalCE.Data.Signals), 2, "expected at least 2 signals from Ruptela OIDs 96+97")

	// Verify specific signal values
	signalsByName := make(map[string]float64)
	for _, s := range signalCE.Data.Signals {
		signalsByName[s.Name] = s.ValueNumber
	}
	assert.Contains(t, signalsByName, "powertrainCombustionEngineECT")
	assert.Contains(t, signalsByName, "exteriorAirTemperature")
	assert.InDelta(t, 67.0, signalsByName["powertrainCombustionEngineECT"], 0.01, "OID 96: 0x6B=107, 107-40=67°C")
	assert.InDelta(t, 20.0, signalsByName["exteriorAirTemperature"], 0.01, "OID 97: 0x3C=60, 60-40=20°C")

	// ── 2. Kafka events topic ───────────────────────────────────
	eventMsgs := consumeKafka(t, "topic.device.events", 10*time.Second)
	var eventCE *vss.EventCloudEvent
	for _, msg := range eventMsgs {
		ce := parseEventCE(t, msg)
		if ce.Subject == subject {
			eventCE = &ce
			break
		}
	}
	require.NotNil(t, eventCE, "event CloudEvent not found in Kafka events topic")
	assert.Equal(t, "1.0", eventCE.SpecVersion)
	assert.Equal(t, "dimo.event", eventCE.Type)
	assert.Equal(t, ruptelaSourceAddress, eventCE.Source)
	// OIDs 135 (braking), 136 (acceleration), 143 (cornering) should produce 3 events
	require.Len(t, eventCE.Data.Events, 3, "expected exactly 3 events from Ruptela OIDs 135+136+143")

	kafkaEventNames := make(map[string]bool)
	for _, e := range eventCE.Data.Events {
		kafkaEventNames[e.Name] = true
	}
	assert.True(t, kafkaEventNames["behavior.harshBraking"], "missing harshBraking event")
	assert.True(t, kafkaEventNames["behavior.harshAcceleration"], "missing harshAcceleration event")
	assert.True(t, kafkaEventNames["behavior.harshCornering"], "missing harshCornering event")

	// ── 3. ClickHouse signal table ──────────────────────────────
	signalRows := querySignals(t, subject)
	// Should match the number of signals in the Kafka CE
	require.Len(t, signalRows, len(signalCE.Data.Signals),
		"ClickHouse signal row count should match Kafka signal count")

	chSignalsByName := make(map[string]SignalRow)
	for _, r := range signalRows {
		chSignalsByName[r.Name] = r
		assert.Equal(t, ruptelaSourceAddress, r.Source, "source mismatch for signal %s", r.Name)
		assert.NotEmpty(t, r.CloudEventID, "cloud_event_id should be set for signal %s", r.Name)
	}
	require.Contains(t, chSignalsByName, "powertrainCombustionEngineECT")
	assert.InDelta(t, 67.0, chSignalsByName["powertrainCombustionEngineECT"].ValueNumber, 0.01)
	require.Contains(t, chSignalsByName, "exteriorAirTemperature")
	assert.InDelta(t, 20.0, chSignalsByName["exteriorAirTemperature"].ValueNumber, 0.01)

	// ── 4. ClickHouse event table ───────────────────────────────
	eventRows := queryEvents(t, subject)
	// Should match the number of events in the Kafka CE
	require.Len(t, eventRows, len(eventCE.Data.Events),
		"ClickHouse event row count should match Kafka event count")

	chEventNames := make(map[string]bool)
	for _, r := range eventRows {
		chEventNames[r.Name] = true
		assert.Equal(t, ruptelaSourceAddress, r.Source, "source mismatch for event %s", r.Name)
		assert.NotEmpty(t, r.CloudEventID, "cloud_event_id should be set for event %s", r.Name)
		assert.Equal(t, "dimo.event", r.Type, "type mismatch for event %s", r.Name)
	}
	assert.True(t, chEventNames["behavior.harshBraking"], "missing harshBraking in ClickHouse")
	assert.True(t, chEventNames["behavior.harshAcceleration"], "missing harshAcceleration in ClickHouse")
	assert.True(t, chEventNames["behavior.harshCornering"], "missing harshCornering in ClickHouse")

	// ── 5. S3 (MinIO) parquet file ──────────────────────────────
	// Wait for parquet batch flush (5s period + buffer)
	time.Sleep(6 * time.Second)
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqFound int
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqFound++
				assert.Equal(t, ruptelaSourceAddress, ev.Source)
				assert.Equal(t, "1.0", ev.SpecVersion)
				// Should be either dimo.status or dimo.event
				assert.Contains(t, []string{"dimo.status", "dimo.event"}, ev.Type,
					"unexpected CE type in parquet: %s", ev.Type)
			}
		}
	}
	// Ruptela with signals+events produces 2 CloudEvents (dimo.status + dimo.event)
	assert.Equal(t, 2, pqFound, "expected 2 CloudEvents in parquet (dimo.status + dimo.event)")
}
