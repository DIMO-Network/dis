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

func TestTeslaFleetAPINoVIN(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:37"

	// Tesla Fleet API payload without VIN — should produce signals but no fingerprint.
	payload := map[string]any{
		"id":          "test-tesla-no-vin-001",
		"source":      teslaSourceAddress,
		"producer":    "did:erc721:137:0x9c94C395cBcBDe662235E0A9d3bB87Ad708561BA:10",
		"specversion": "1.0",
		"subject":     subject,
		"time":        "2024-11-04T12:00:00Z",
		"type":        "dimo.status",
		"signature":   "0xdeadbeef",
		"dataversion": "fleet_api/v1.0.0",
		"data": map[string]any{
			"charge_state": map[string]any{
				"battery_level":       23,
				"battery_range":       341,
				"charge_energy_added": 42,
				"charge_limit_soc":    80,
				"charging_state":      "Charging",
				"timestamp":           1730728800000,
			},
			"climate_state": map[string]any{
				"outside_temp": 19,
				"timestamp":    1730728802000,
			},
			"drive_state": map[string]any{
				"latitude":  38.89,
				"longitude": 77.03,
				"power":     -7,
				"speed":     25,
				"timestamp": 1730738800000,
			},
			"vehicle_state": map[string]any{
				"odometer":         5633,
				"tpms_pressure_fl": 3.12,
				"tpms_pressure_fr": 3.09,
				"tpms_pressure_rl": 2.98,
				"tpms_pressure_rr": 2.99,
				"timestamp":        1730728805000,
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	signalOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLSWithConfig(t, payloadBytes, teslaTLSConfig)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "DIS should accept Tesla payload without VIN")

	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka signals topic — 1 signal payload ───────────────
	signalMsgs := consumeKafka(t, "topic.device.signals", signalOffset, 10*time.Second)
	require.Len(t, signalMsgs, 1, "expected exactly 1 signal message")
	signalCE := func() *vss.SignalCloudEvent { ce := parseSignalCE(t, signalMsgs[0]); return &ce }()
	require.Equal(t, "dimo.signals", signalCE.Type)
	require.Equal(t, teslaSourceAddress, signalCE.Source)
	require.Equal(t, subject, signalCE.Subject)

	// Tesla Fleet API produces 14 signals (same as with-VIN payload minus VIN-related changes)
	require.Len(t, signalCE.Data.Signals, 14, "expected 14 signals from Tesla Fleet API payload")

	signalsByName := make(map[string]float64)
	for _, s := range signalCE.Data.Signals {
		signalsByName[s.Name] = s.ValueNumber
	}
	require.InDelta(t, 23.0, signalsByName["powertrainTractionBatteryStateOfChargeCurrent"], 0.01, "battery_level=23")
	require.InDelta(t, 19.0, signalsByName["exteriorAirTemperature"], 0.01, "outside_temp=19")
	require.InDelta(t, 312.0, signalsByName["chassisAxleRow1WheelLeftTirePressure"], 0.01, "tpms_pressure_fl=3.12 → 312 kPa")
	require.InDelta(t, 42.0, signalsByName["powertrainTractionBatteryChargingAddedEnergy"], 0.01, "charge_energy_added=42")
	require.InDelta(t, 80.0, signalsByName["powertrainTractionBatteryChargingChargeLimit"], 0.01, "charge_limit_soc=80")
	require.InDelta(t, 1.0, signalsByName["powertrainTractionBatteryChargingIsCharging"], 0.01, "charging_state=Charging → 1")
	require.InDelta(t, 40.2336, signalsByName["speed"], 0.01, "speed=25 mph → 40.2336 km/h")

	// ── 2. ClickHouse signal table — 14 signal rows ─────────────
	signalRows := querySignals(t, subject)
	require.Len(t, signalRows, 14, "expected 14 signal rows in ClickHouse")

	chSignalsByName := make(map[string]SignalRow)
	for _, r := range signalRows {
		chSignalsByName[r.Name] = r
		require.Equal(t, teslaSourceAddress, r.Source, "source mismatch for signal %s", r.Name)
	}
	require.InDelta(t, 23.0, chSignalsByName["powertrainTractionBatteryStateOfChargeCurrent"].ValueNumber, 0.01)
	require.InDelta(t, 19.0, chSignalsByName["exteriorAirTemperature"].ValueNumber, 0.01)

	// ── 3. No events for Tesla ──────────────────────────────────
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 0, "Tesla should produce no events")

	// ── 4. S3 (MinIO) parquet — only 1 CE (dimo.status, no VIN → no fingerprint)
	time.Sleep(750 * time.Millisecond)
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqFound int
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqFound++
				require.Equal(t, teslaSourceAddress, ev.Source)
				require.Equal(t, "dimo.status", ev.Type, "only dimo.status CE expected (no VIN)")
			}
		}
	}
	require.Equal(t, 1, pqFound, "expected 1 CloudEvent in parquet (dimo.status only, no fingerprint)")

	// ── 5. ClickHouse cloud_event table — 1 index row ───────────
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 1, "expected 1 cloud_event index row (dimo.status only)")
	require.Equal(t, "dimo.status", ceRows[0].EventType)
	require.Equal(t, teslaSourceAddress, ceRows[0].Source)
}

func TestTeslaFleetAPIFullPipeline(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:9001"

	// Tesla Fleet API payload — already in CloudEvent format.
	// Signal data in charge_state, climate_state, drive_state, vehicle_state.
	// VIN present → produces both dimo.status and dimo.fingerprint CEs.
	payload := map[string]any{
		"id":          "test-tesla-fleet-api-001",
		"source":      teslaSourceAddress,
		"producer":    "did:erc721:137:0x9c94C395cBcBDe662235E0A9d3bB87Ad708561BA:10",
		"specversion": "1.0",
		"subject":     subject,
		"time":        "2024-11-04T12:00:00Z",
		"type":        "dimo.status",
		"signature":   "0xdeadbeef",
		"dataversion": "fleet_api/v1.0.0",
		"data": map[string]any{
			"vin":         "VF33E1EB4K55F700D",
			"id":          234234,
			"user_id":     32425456,
			"vehicle_id":  33,
			"access_type": "OWNER",
			"charge_state": map[string]any{
				"battery_level":       23,
				"battery_range":       341,
				"charge_energy_added": 42,
				"charge_limit_soc":    80,
				"charging_state":      "Charging",
				"timestamp":           1730728800000,
			},
			"climate_state": map[string]any{
				"outside_temp": 19,
				"timestamp":    1730728802000,
			},
			"drive_state": map[string]any{
				"latitude":  38.89,
				"longitude": 77.03,
				"power":     -7,
				"speed":     25,
				"timestamp": 1730738800000,
			},
			"vehicle_state": map[string]any{
				"odometer":         5633,
				"tpms_pressure_fl": 3.12,
				"tpms_pressure_fr": 3.09,
				"tpms_pressure_rl": 2.98,
				"tpms_pressure_rr": 2.99,
				"timestamp":        1730728805000,
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	signalOffset := kafkaEndOffset(t, "topic.device.signals")

	// Send via Tesla mTLS client cert (CN = Tesla source address)
	resp := postMTLSWithConfig(t, payloadBytes, teslaTLSConfig)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "DIS should accept valid Tesla payload")

	// Wait for pipeline processing
	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka signals topic — 1 signal payload ───────────────
	signalMsgs := consumeKafka(t, "topic.device.signals", signalOffset, 10*time.Second)
	require.Len(t, signalMsgs, 1, "expected exactly 1 signal message")
	signalCE := func() *vss.SignalCloudEvent { ce := parseSignalCE(t, signalMsgs[0]); return &ce }()
	require.Equal(t, "1.0", signalCE.SpecVersion)
	require.Equal(t, "dimo.signals", signalCE.Type)
	require.Equal(t, teslaSourceAddress, signalCE.Source)

	// Tesla Fleet API produces 14 signals from the test payload
	require.Len(t, signalCE.Data.Signals, 14, "expected 14 signals from Tesla Fleet API payload")

	signalsByName := make(map[string]float64)
	for _, s := range signalCE.Data.Signals {
		signalsByName[s.Name] = s.ValueNumber
	}
	require.InDelta(t, 23.0, signalsByName["powertrainTractionBatteryStateOfChargeCurrent"], 0.01, "battery_level=23")
	require.InDelta(t, 19.0, signalsByName["exteriorAirTemperature"], 0.01, "outside_temp=19")
	require.InDelta(t, 312.0, signalsByName["chassisAxleRow1WheelLeftTirePressure"], 0.01, "tpms_pressure_fl=3.12 → 312 kPa")
	require.InDelta(t, 42.0, signalsByName["powertrainTractionBatteryChargingAddedEnergy"], 0.01, "charge_energy_added=42")
	require.InDelta(t, 80.0, signalsByName["powertrainTractionBatteryChargingChargeLimit"], 0.01, "charge_limit_soc=80")
	require.InDelta(t, 1.0, signalsByName["powertrainTractionBatteryChargingIsCharging"], 0.01, "charging_state=Charging → 1")
	require.InDelta(t, 40.2336, signalsByName["speed"], 0.01, "speed=25 mph → 40.2336 km/h")

	// ── 2. Kafka events topic — Tesla produces no events ─────────
	// (Tesla module has no EventConvert registered)

	// ── 3. ClickHouse signal table — 14 signal rows ─────────────
	signalRows := querySignals(t, subject)
	require.Len(t, signalRows, 14, "expected 14 signal rows in ClickHouse")

	chSignalsByName := make(map[string]SignalRow)
	for _, r := range signalRows {
		chSignalsByName[r.Name] = r
		require.Equal(t, teslaSourceAddress, r.Source, "source mismatch for signal %s", r.Name)
	}
	require.InDelta(t, 23.0, chSignalsByName["powertrainTractionBatteryStateOfChargeCurrent"].ValueNumber, 0.01)
	require.InDelta(t, 19.0, chSignalsByName["exteriorAirTemperature"].ValueNumber, 0.01)

	// ── 4. ClickHouse event table — 0 event rows ────────────────
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 0, "Tesla should produce no events")

	// ── 5. S3 (MinIO) parquet — 2 CEs (dimo.status + dimo.fingerprint)
	time.Sleep(750 * time.Millisecond)
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqEvents []cloudevent.StoredEvent
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqEvents = append(pqEvents, ev)
				require.Equal(t, teslaSourceAddress, ev.Source)
				require.Equal(t, "1.0", ev.SpecVersion)
			}
		}
	}
	t.Logf("Found %d parquet rows for subject %s", len(pqEvents), subject)
	// Tesla produces 1 parquet entry (status+fingerprint share the same ID, deduplicated)
	require.Len(t, pqEvents, 1, "expected 1 CloudEvent in parquet (dimo.status, fingerprint deduplicated)")
	require.Equal(t, "dimo.status", pqEvents[0].Type, "parquet entry should be dimo.status (fingerprint deduplicated)")

	// ── 6. ClickHouse cloud_event table — 2 index rows ──────────
	ceRows := queryCloudEvents(t, subject)
	require.Len(t, ceRows, 2, "expected 2 cloud_event index rows (dimo.status + dimo.fingerprint)")

	ceByType := make(map[string]CloudEventRow)
	for _, r := range ceRows {
		ceByType[r.EventType] = r
		require.Equal(t, teslaSourceAddress, r.Source)
		require.NotEmpty(t, r.IndexKey, "index_key should be set")
	}
	require.Contains(t, ceByType, "dimo.status", "missing dimo.status in cloud_event table")
	require.Contains(t, ceByType, "dimo.fingerprint", "missing dimo.fingerprint in cloud_event table")

	statusID := ceByType["dimo.status"].ID
	fingerprintID := ceByType["dimo.fingerprint"].ID

	// Status and fingerprint share the same ID (identical payload, deduplicated in parquet)
	require.Equal(t, statusID, fingerprintID,
		"dimo.status and dimo.fingerprint must share the same cloud event ID")
	require.Equal(t, ceByType["dimo.status"].IndexKey, ceByType["dimo.fingerprint"].IndexKey,
		"dimo.status and dimo.fingerprint must point to the same parquet entry")
}
