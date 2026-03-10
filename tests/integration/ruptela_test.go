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
	clearMinIOObjects(t, "cloudevent/valid/")

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:444"

	// Ruptela status payload with both signals and event counters.
	// Signal 96 = powertrainCombustionEngineECT (hex 0x6B=107, 107-40=67°C)
	// Signal 97 = exteriorAirTemperature (hex 0x3C=60, 60-40=20°C)
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
	t.Logf("Input payload: %s", string(payloadBytes))

	signalOffset := kafkaEndOffset(t, "topic.device.signals")
	eventOffset := kafkaEndOffset(t, "topic.device.events")

	// Use Ruptela mTLS client cert (CN = Ruptela source address)
	resp := postMTLSWithConfig(t, payloadBytes, ruptelaTLSConfig)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "DIS should accept valid Ruptela payload")

	// Wait for pipeline processing
	time.Sleep(5 * time.Second)

	// ── 1. Kafka signals topic — 1 signal payload ───────────────
	signalMsgs := consumeKafka(t, "topic.device.signals", signalOffset, 10*time.Second)
	require.Len(t, signalMsgs, 1, "expected exactly 1 signal message")
	signalCE := func() *vss.SignalCloudEvent { ce := parseSignalCE(t, signalMsgs[0]); return &ce }()
	assert.Equal(t, "1.0", signalCE.SpecVersion)
	assert.Equal(t, "dimo.signals", signalCE.Type)
	assert.Equal(t, ruptelaSourceAddress, signalCE.Source)

	require.Len(t, signalCE.Data.Signals, 2, "expected exactly 2 signals from Ruptela OIDs 96+97")

	signalsByName := make(map[string]float64)
	for _, s := range signalCE.Data.Signals {
		signalsByName[s.Name] = s.ValueNumber
	}
	assert.InDelta(t, 67.0, signalsByName["powertrainCombustionEngineECT"], 0.01, "OID 96: 0x6B=107, 107-40=67°C")
	assert.InDelta(t, 20.0, signalsByName["exteriorAirTemperature"], 0.01, "OID 97: 0x3C=60, 60-40=20°C")

	// ── 2. Kafka events topic — 1 event payload ─────────────────
	eventMsgs := consumeKafka(t, "topic.device.events", eventOffset, 10*time.Second)
	require.Len(t, eventMsgs, 1, "expected exactly 1 event message")
	eventCE := func() *vss.EventCloudEvent { ce := parseEventCE(t, eventMsgs[0]); return &ce }()
	assert.Equal(t, "1.0", eventCE.SpecVersion)
	assert.Equal(t, "dimo.events", eventCE.Type)
	assert.Equal(t, ruptelaSourceAddress, eventCE.Source)

	// OID 135 value "35" = harsh:5 + extreme:3, producing 2 events
	require.Len(t, eventCE.Data.Events, 4, "expected 4 events: harshBraking + extremeBraking (OID 135) + harshAcceleration (136) + harshCornering (143)")

	kafkaEventNames := make(map[string]bool)
	for _, e := range eventCE.Data.Events {
		kafkaEventNames[e.Name] = true
	}
	assert.True(t, kafkaEventNames["behavior.harshBraking"], "missing harshBraking event")
	assert.True(t, kafkaEventNames["behavior.extremeBraking"], "missing extremeBraking event")
	assert.True(t, kafkaEventNames["behavior.harshAcceleration"], "missing harshAcceleration event")
	assert.True(t, kafkaEventNames["behavior.harshCornering"], "missing harshCornering event")

	// ── 3. ClickHouse signal table — 2 signal rows ──────────────
	signalRows := querySignals(t, subject)
	require.Len(t, signalRows, 2, "expected exactly 2 signal rows in ClickHouse")

	chSignalsByName := make(map[string]SignalRow)
	for _, r := range signalRows {
		chSignalsByName[r.Name] = r
		assert.Equal(t, ruptelaSourceAddress, r.Source, "source mismatch for signal %s", r.Name)
	}
	assert.InDelta(t, 67.0, chSignalsByName["powertrainCombustionEngineECT"].ValueNumber, 0.01)
	assert.InDelta(t, 20.0, chSignalsByName["exteriorAirTemperature"].ValueNumber, 0.01)

	// ── 4. ClickHouse event table — 4 event rows ────────────────
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 4, "expected 4 event rows in ClickHouse")

	chEventNames := make(map[string]bool)
	for _, r := range eventRows {
		chEventNames[r.Name] = true
		assert.Equal(t, ruptelaSourceAddress, r.Source, "source mismatch for event %s", r.Name)
		assert.Equal(t, "dimo.event", r.Type, "type mismatch for event %s", r.Name)
	}
	assert.True(t, chEventNames["behavior.harshBraking"], "missing harshBraking in ClickHouse")
	assert.True(t, chEventNames["behavior.extremeBraking"], "missing extremeBraking in ClickHouse")
	assert.True(t, chEventNames["behavior.harshAcceleration"], "missing harshAcceleration in ClickHouse")
	assert.True(t, chEventNames["behavior.harshCornering"], "missing harshCornering in ClickHouse")

	// ── 5. S3 (MinIO) parquet — 1 parquet file ──────────────────
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
			}
		}
	}
	t.Logf("Found %d parquet rows for subject %s", pqFound, subject)
	// Ruptela produces 3 CEs: dimo.status, dimo.fingerprint, dimo.events
	assert.Equal(t, 3, pqFound, "expected 3 CloudEvents in parquet (dimo.status + dimo.fingerprint + dimo.events)")

	// ── 6. ClickHouse cloud_event table — 3 index rows ──────────
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 3, "expected 3 cloud_event index rows (dimo.status + dimo.fingerprint + dimo.events)")

	ceTypes := make(map[string]bool)
	for _, r := range ceRows {
		ceTypes[r.EventType] = true
		assert.Equal(t, ruptelaSourceAddress, r.Source)
		assert.NotEmpty(t, r.IndexKey, "index_key should be set")
	}
	assert.True(t, ceTypes["dimo.status"], "missing dimo.status in cloud_event table")
	assert.True(t, ceTypes["dimo.fingerprint"], "missing dimo.fingerprint in cloud_event table")
	assert.True(t, ceTypes["dimo.events"], "missing dimo.events in cloud_event table")
}
