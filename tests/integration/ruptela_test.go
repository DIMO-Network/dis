//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/require"
)

func TestRuptelaCommandEngineBlock(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:555"
	clearClickHouseForSubject(t, subject)

	// Command payload with engine block signal (405 = "1" means block).
	// Also includes behavior counters (135/136/143) which should still produce events.
	payload := map[string]any{
		"ds":             "r/v0/cmd",
		"signature":      "0xdeadbeef",
		"subject":        subject,
		"vehicleTokenId": 555,
		"deviceTokenId":  10,
		"time":           "2024-09-27T08:33:26Z",
		"data": map[string]any{
			"signals": map[string]string{
				"135": "0",
				"136": "0",
				"143": "0",
				"405": "1", // engine block (non-zero = block)
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	eventOffset := kafkaEndOffset(t, "topic.device.events")

	resp := postMTLSWithConfig(t, payloadBytes, ruptelaTLSConfig)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "DIS should accept valid Ruptela command payload")

	// Wait for pipeline processing
	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka events topic — 1 event payload with engine block ──
	eventMsgs := consumeKafka(t, "topic.device.events", eventOffset, 10*time.Second)
	require.Len(t, eventMsgs, 1, "expected exactly 1 event message")
	eventCE := func() *vss.EventCloudEvent { ce := parseEventCE(t, eventMsgs[0]); return &ce }()
	require.Equal(t, "1.0", eventCE.SpecVersion)
	require.Equal(t, "dimo.events", eventCE.Type)
	require.Equal(t, ruptelaSourceAddress, eventCE.Source)

	require.Len(t, eventCE.Data.Events, 1, "expected 1 event: engineBlock from OID 405")

	require.Equal(t, "security.engineBlock", eventCE.Data.Events[0].Name)

	// ── 2. ClickHouse event table — 1 engine block row ──────────
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 1, "expected 1 event row in ClickHouse")
	require.Equal(t, "security.engineBlock", eventRows[0].Name)
	require.Equal(t, ruptelaSourceAddress, eventRows[0].Source)
	require.Equal(t, "dimo.event", eventRows[0].Type)

	// ── 3. S3 (MinIO) parquet — only dimo.events CE ─────────────
	time.Sleep(750 * time.Millisecond)
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqFound int
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqFound++
				require.Equal(t, ruptelaSourceAddress, ev.Source)
				require.Equal(t, "1.0", ev.SpecVersion)
			}
		}
	}
	t.Logf("Found %d parquet rows for subject %s", pqFound, subject)
	// Command DS produces only dimo.events CE (no status or fingerprint)
	require.Equal(t, 1, pqFound, "expected 1 CloudEvent in parquet (dimo.events only for cmd DS)")

	// ── 4. ClickHouse cloud_event table — 1 index row ───────────
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 1, "expected 1 cloud_event index row (dimo.events only)")
	require.Equal(t, "dimo.events", ceRows[0].EventType)
	require.Equal(t, ruptelaSourceAddress, ceRows[0].Source)
	require.NotEmpty(t, ceRows[0].IndexKey, "index_key should be set")
}

func TestRuptelaCommandEngineUnblock(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:556"
	clearClickHouseForSubject(t, subject)

	// Command payload with engine unblock signal (405 = "0" means unblock).
	payload := map[string]any{
		"ds":             "r/v0/cmd",
		"signature":      "0xdeadbeef",
		"subject":        subject,
		"vehicleTokenId": 556,
		"deviceTokenId":  10,
		"time":           "2024-09-27T08:33:26Z",
		"data": map[string]any{
			"signals": map[string]string{
				"135": "0",
				"136": "0",
				"143": "0",
				"405": "0", // engine unblock (zero = unblock)
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	eventOffset := kafkaEndOffset(t, "topic.device.events")

	resp := postMTLSWithConfig(t, payloadBytes, ruptelaTLSConfig)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "DIS should accept valid Ruptela command payload")

	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka events topic — 1 event payload with engine unblock ──
	eventMsgs := consumeKafka(t, "topic.device.events", eventOffset, 10*time.Second)
	require.Len(t, eventMsgs, 1, "expected exactly 1 event message")
	eventCE := func() *vss.EventCloudEvent { ce := parseEventCE(t, eventMsgs[0]); return &ce }()
	require.Equal(t, "1.0", eventCE.SpecVersion)
	require.Equal(t, "dimo.events", eventCE.Type)
	require.Equal(t, ruptelaSourceAddress, eventCE.Source)

	require.Len(t, eventCE.Data.Events, 1, "expected 1 event: engineUnblock from OID 405")

	require.Equal(t, "security.engineUnblock", eventCE.Data.Events[0].Name)

	// ── 2. ClickHouse event table — 1 engine unblock row ────────
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 1, "expected 1 event row in ClickHouse")
	require.Equal(t, "security.engineUnblock", eventRows[0].Name)
	require.Equal(t, ruptelaSourceAddress, eventRows[0].Source)
	require.Equal(t, "dimo.event", eventRows[0].Type)

	// ── 3. S3 (MinIO) parquet — only dimo.events CE ─────────────
	time.Sleep(750 * time.Millisecond)
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqFound int
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqFound++
				require.Equal(t, ruptelaSourceAddress, ev.Source)
				require.Equal(t, "1.0", ev.SpecVersion)
			}
		}
	}
	t.Logf("Found %d parquet rows for subject %s", pqFound, subject)
	require.Equal(t, 1, pqFound, "expected 1 CloudEvent in parquet (dimo.events only for cmd DS)")

	// ── 4. ClickHouse cloud_event table — 1 index row ───────────
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 1, "expected 1 cloud_event index row (dimo.events only)")
	require.Equal(t, "dimo.events", ceRows[0].EventType)
	require.Equal(t, ruptelaSourceAddress, ceRows[0].Source)
	require.NotEmpty(t, ceRows[0].IndexKey, "index_key should be set")
}

func TestRuptelaStatusDS_Signal405_NoEngineEvent(t *testing.T) {
	// Signal 405 from status DS ("r/v0/s") should NOT produce engine block/unblock events.
	// Only the command DS ("r/v0/cmd") should trigger engine security events.
	clearMinIOObjects(t, "cloudevent/valid/")
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:557"
	clearClickHouseForSubject(t, subject)

	payload := map[string]any{
		"ds":             "r/v0/s",
		"signature":      "0xdeadbeef",
		"subject":        subject,
		"vehicleTokenId": 557,
		"deviceTokenId":  10,
		"time":           "2024-09-27T08:33:26Z",
		"data": map[string]any{
			"signals": map[string]string{
				"96":  "6B", // include a real signal so the payload is valid
				"135": "0",
				"136": "0",
				"143": "0",
				"405": "1", // engine block value, but from status DS — should be ignored
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	eventOffset := kafkaEndOffset(t, "topic.device.events")

	resp := postMTLSWithConfig(t, payloadBytes, ruptelaTLSConfig)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "DIS should accept valid Ruptela status payload")

	time.Sleep(750 * time.Millisecond)

	// No events should be produced — behavior counters are all zero and
	// engine block from status DS should be ignored.
	eventMsgs := consumeKafka(t, "topic.device.events", eventOffset, 10*time.Second)
	require.Empty(t, eventMsgs, "signal 405 from status DS should NOT produce engine events")

	// ClickHouse event table should have no engine events for this subject
	eventRows := queryEvents(t, subject)
	for _, r := range eventRows {
		require.NotEqual(t, "security.engineBlock", r.Name, "engine block should not appear from status DS")
		require.NotEqual(t, "security.engineUnblock", r.Name, "engine unblock should not appear from status DS")
	}
}

func TestRuptelaFullPipeline(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:444"
	clearClickHouseForSubject(t, subject)

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
	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka signals topic — 1 signal payload ───────────────
	signalMsgs := consumeKafka(t, "topic.device.signals", signalOffset, 10*time.Second)
	require.Len(t, signalMsgs, 1, "expected exactly 1 signal message")
	signalCE := func() *vss.SignalCloudEvent { ce := parseSignalCE(t, signalMsgs[0]); return &ce }()
	require.Equal(t, "1.0", signalCE.SpecVersion)
	require.Equal(t, "dimo.signals", signalCE.Type)
	require.Equal(t, ruptelaSourceAddress, signalCE.Source)

	require.Len(t, signalCE.Data.Signals, 2, "expected exactly 2 signals from Ruptela OIDs 96+97")

	signalsByName := make(map[string]float64)
	for _, s := range signalCE.Data.Signals {
		signalsByName[s.Name] = s.ValueNumber
	}
	require.InDelta(t, 67.0, signalsByName["powertrainCombustionEngineECT"], 0.01, "OID 96: 0x6B=107, 107-40=67°C")
	require.InDelta(t, 20.0, signalsByName["exteriorAirTemperature"], 0.01, "OID 97: 0x3C=60, 60-40=20°C")

	// ── 2. Kafka events topic — 1 event payload ─────────────────
	eventMsgs := consumeKafka(t, "topic.device.events", eventOffset, 10*time.Second)
	require.Len(t, eventMsgs, 1, "expected exactly 1 event message")
	eventCE := func() *vss.EventCloudEvent { ce := parseEventCE(t, eventMsgs[0]); return &ce }()
	require.Equal(t, "1.0", eventCE.SpecVersion)
	require.Equal(t, "dimo.events", eventCE.Type)
	require.Equal(t, ruptelaSourceAddress, eventCE.Source)

	// OID 135 value "35" = harsh:5 + extreme:3, producing 2 events
	require.Len(t, eventCE.Data.Events, 4, "expected 4 events: harshBraking + extremeBraking (OID 135) + harshAcceleration (136) + harshCornering (143)")

	kafkaEventNames := make(map[string]bool)
	for _, e := range eventCE.Data.Events {
		kafkaEventNames[e.Name] = true
	}
	require.True(t, kafkaEventNames["behavior.harshBraking"], "missing harshBraking event")
	require.True(t, kafkaEventNames["behavior.extremeBraking"], "missing extremeBraking event")
	require.True(t, kafkaEventNames["behavior.harshAcceleration"], "missing harshAcceleration event")
	require.True(t, kafkaEventNames["behavior.harshCornering"], "missing harshCornering event")

	// ── 3. ClickHouse signal table — 2 signal rows ──────────────
	signalRows := querySignals(t, subject)
	require.Len(t, signalRows, 2, "expected exactly 2 signal rows in ClickHouse")

	chSignalsByName := make(map[string]SignalRow)
	for _, r := range signalRows {
		chSignalsByName[r.Name] = r
		require.Equal(t, ruptelaSourceAddress, r.Source, "source mismatch for signal %s", r.Name)
	}
	require.InDelta(t, 67.0, chSignalsByName["powertrainCombustionEngineECT"].ValueNumber, 0.01)
	require.InDelta(t, 20.0, chSignalsByName["exteriorAirTemperature"].ValueNumber, 0.01)

	// ── 4. ClickHouse event table — 4 event rows ────────────────
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 4, "expected 4 event rows in ClickHouse")

	chEventNames := make(map[string]bool)
	for _, r := range eventRows {
		chEventNames[r.Name] = true
		require.Equal(t, ruptelaSourceAddress, r.Source, "source mismatch for event %s", r.Name)
		require.Equal(t, "dimo.event", r.Type, "type mismatch for event %s", r.Name)
	}
	require.True(t, chEventNames["behavior.harshBraking"], "missing harshBraking in ClickHouse")
	require.True(t, chEventNames["behavior.extremeBraking"], "missing extremeBraking in ClickHouse")
	require.True(t, chEventNames["behavior.harshAcceleration"], "missing harshAcceleration in ClickHouse")
	require.True(t, chEventNames["behavior.harshCornering"], "missing harshCornering in ClickHouse")

	// ── 5. S3 (MinIO) parquet — 1 parquet file ──────────────────
	time.Sleep(750 * time.Millisecond)
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqEvents []cloudevent.RawEvent
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqEvents = append(pqEvents, ev)
				require.Equal(t, ruptelaSourceAddress, ev.Source)
				require.Equal(t, "1.0", ev.SpecVersion)
			}
		}
	}
	t.Logf("Found %d parquet rows for subject %s", len(pqEvents), subject)
	// Ruptela status+fingerprint share the same ID (deduplicated), events has a separate ID
	require.Len(t, pqEvents, 2, "expected 2 CloudEvents in parquet (dimo.status + dimo.events, fingerprint deduplicated)")

	pqByType := make(map[string]cloudevent.RawEvent)
	for _, ev := range pqEvents {
		pqByType[ev.Type] = ev
	}
	require.Contains(t, pqByType, "dimo.status", "missing dimo.status in parquet")
	require.Contains(t, pqByType, "dimo.events", "missing dimo.events in parquet")
	// Fingerprint should NOT be in parquet (deduplicated with status)
	require.NotContains(t, pqByType, "dimo.fingerprint", "dimo.fingerprint should be deduplicated out of parquet")

	// ── 6. ClickHouse cloud_event table — 3 index rows ──────────
	// ID relationships:
	//   statusID  — shared by dimo.status and dimo.fingerprint (same raw payload)
	//   eventsID  — unique to dimo.events (different logical data)
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 3, "expected 3 cloud_event index rows (dimo.status + dimo.fingerprint + dimo.events)")

	ceByType := make(map[string]CloudEventRow)
	for _, r := range ceRows {
		ceByType[r.EventType] = r
		require.Equal(t, ruptelaSourceAddress, r.Source)
		require.NotEmpty(t, r.IndexKey, "index_key should be set")
	}
	require.Contains(t, ceByType, "dimo.status", "missing dimo.status in cloud_event table")
	require.Contains(t, ceByType, "dimo.fingerprint", "missing dimo.fingerprint in cloud_event table")
	require.Contains(t, ceByType, "dimo.events", "missing dimo.events in cloud_event table")

	statusID := ceByType["dimo.status"].ID
	fingerprintID := ceByType["dimo.fingerprint"].ID
	eventsID := ceByType["dimo.events"].ID

	// Status and fingerprint share the same ID (identical payload, deduplicated in parquet)
	require.Equal(t, statusID, fingerprintID,
		"dimo.status and dimo.fingerprint must share the same cloud event ID")
	require.Equal(t, ceByType["dimo.status"].IndexKey, ceByType["dimo.fingerprint"].IndexKey,
		"dimo.status and dimo.fingerprint must point to the same parquet entry")

	// Events has its own separate ID
	require.NotEqual(t, statusID, eventsID,
		"dimo.events must have a different cloud event ID than dimo.status/dimo.fingerprint")
	require.NotEqual(t, ceByType["dimo.status"].IndexKey, ceByType["dimo.events"].IndexKey,
		"dimo.events must point to a different parquet entry than dimo.status/dimo.fingerprint")

	// Each individual event row in ClickHouse stores the dimo.events CE ID as its cloudEventId.
	// This links each event back to the dimo.events CE that carried it through the pipeline.
	for _, r := range eventRows {
		require.Equal(t, eventsID, r.CloudEventID,
			"event row %q cloudEventId must equal the dimo.events CE ID (%s), not the dimo.status CE ID (%s)",
			r.Name, eventsID, statusID)
	}

	// The Kafka events payload also embeds this same eventsID inside each event's cloudEventId field.
	for _, ev := range eventCE.Data.Events {
		require.Equal(t, eventsID, ev.CloudEventID,
			"Kafka event %q cloudEventId must equal the dimo.events CE ID (%s)", ev.Name, eventsID)
	}
}
