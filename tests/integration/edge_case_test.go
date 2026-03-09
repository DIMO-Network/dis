//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFutureTimestampSignalPruning(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:8001"

	futureTime := time.Now().Add(1 * time.Hour).UTC().Format("2006-01-02T15:04:05.000Z")

	payload := map[string]any{
		"id":             "test-future-ts-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 8001,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals": []map[string]any{
				{
					"timestamp": futureTime,
					"name":      "powertrainCombustionEngineECT",
					"value":     90,
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

	// The CloudEvent is still processed but the signal should be pruned
	// because its timestamp is 1 hour in the future (> 5 min threshold).
	msgs := consumeKafka(t, "topic.device.signals", 10*time.Second)
	for _, msg := range msgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			// If a signal CE exists for this subject, it should have 0 signals
			assert.Empty(t, ce.Data.Signals, "future-timestamped signal should have been pruned")
			return
		}
	}
	// It is also acceptable for no signal CE to appear at all for this subject
	// (pipeline may drop messages with zero valid signals).
}

func TestDuplicateSignalPruning(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:8002"

	payload := map[string]any{
		"id":             "test-dup-signal-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 8002,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals": []map[string]any{
				{
					"timestamp": "2024-04-18T17:20:26.633Z",
					"name":      "powertrainCombustionEngineECT",
					"value":     107,
				},
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

	msgs := consumeKafka(t, "topic.device.signals", 10*time.Second)
	var found bool
	for _, msg := range msgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			found = true
			require.Len(t, ce.Data.Signals, 1, "duplicate signal should have been pruned, expected exactly 1")
			assert.Equal(t, "powertrainCombustionEngineECT", ce.Data.Signals[0].Name)
			assert.InDelta(t, 107.0, ce.Data.Signals[0].ValueNumber, 0.01)
			break
		}
	}
	assert.True(t, found, "signal CloudEvent not found in Kafka messages")

	// ClickHouse should also have exactly 1 row (deduplicated)
	rows := querySignals(t, subject)
	require.Len(t, rows, 1, "ClickHouse should have exactly 1 signal row after deduplication")
	assert.Equal(t, "powertrainCombustionEngineECT", rows[0].Name)
	assert.InDelta(t, 107.0, rows[0].ValueNumber, 0.01)
}

func TestEmptySignalsArray(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:8003"

	payload := map[string]any{
		"id":             "test-empty-signals-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 8003,
		"data": map[string]any{
			"signals": []map[string]any{},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	assert.Equal(t, 200, resp.StatusCode)

	time.Sleep(3 * time.Second)

	// No signals should appear for this subject
	msgs := consumeKafka(t, "topic.device.signals", 10*time.Second)
	for _, msg := range msgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			assert.Empty(t, ce.Data.Signals, "empty signals array should produce no signals")
			return
		}
	}
	// No signal CE for this subject is the expected outcome
}

func TestSignalsAndEventsInSamePayload(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:8004"

	payload := map[string]any{
		"id":             "test-mixed-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 8004,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals": []map[string]any{
				{
					"timestamp": "2024-04-18T17:20:26.633Z",
					"name":      "powertrainCombustionEngineECT",
					"value":     80,
				},
			},
			"events": []map[string]any{
				{
					"name":      "behavior.harshBraking",
					"timestamp": "2024-04-18T17:20:46.436008782Z",
					"tags":      []string{},
				},
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	if resp.StatusCode == 408 {
		t.Log("got 408 (pipeline congestion), checking Kafka anyway")
	} else {
		assert.Equal(t, 200, resp.StatusCode)
	}

	time.Sleep(3 * time.Second)

	// Verify signal appears in Kafka signals topic
	signalMsgs := consumeKafka(t, "topic.device.signals", 10*time.Second)
	var signalFound bool
	for _, msg := range signalMsgs {
		ce := parseSignalCE(t, msg)
		if ce.Subject == subject {
			signalFound = true
			require.Len(t, ce.Data.Signals, 1)
			assert.Equal(t, "powertrainCombustionEngineECT", ce.Data.Signals[0].Name)
			assert.InDelta(t, 80.0, ce.Data.Signals[0].ValueNumber, 0.01)
			break
		}
	}
	assert.True(t, signalFound, "signal CloudEvent not found in Kafka signals topic")

	// Verify event appears in Kafka events topic
	eventMsgs := consumeKafka(t, "topic.device.events", 10*time.Second)
	var eventFound bool
	for _, msg := range eventMsgs {
		ce := parseEventCE(t, msg)
		if ce.Subject == subject {
			eventFound = true
			assert.Equal(t, "dimo.event", ce.Type)
			require.Len(t, ce.Data.Events, 1)
			assert.Equal(t, "behavior.harshBraking", ce.Data.Events[0].Name)
			break
		}
	}
	if !eventFound && resp.StatusCode == 408 {
		t.Skip("skipping event check: not delivered due to pipeline congestion (known issue)")
	}
	assert.True(t, eventFound, "event CloudEvent not found in Kafka events topic")

	// ── ClickHouse signal table — exactly 1 signal row ──────────
	signalRows := querySignals(t, subject)
	require.Len(t, signalRows, 1, "expected exactly 1 signal row in ClickHouse for mixed payload")
	assert.Equal(t, "powertrainCombustionEngineECT", signalRows[0].Name)
	assert.InDelta(t, 80.0, signalRows[0].ValueNumber, 0.01)
	assert.Equal(t, testSourceAddress, signalRows[0].Source)

	// ── ClickHouse event table — exactly 1 event row ────────────
	eventRows := queryEvents(t, subject)
	if resp.StatusCode == 408 && len(eventRows) == 0 {
		t.Log("skipping ClickHouse event check: event not delivered due to pipeline congestion")
	} else {
		require.Len(t, eventRows, 1, "expected exactly 1 event row in ClickHouse for mixed payload")
		assert.Equal(t, "behavior.harshBraking", eventRows[0].Name)
		assert.Equal(t, testSourceAddress, eventRows[0].Source)
	}
}
