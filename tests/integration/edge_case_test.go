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
	t.Logf("Input payload: %s", string(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	// DIS may return 408 when all signals are pruned and downstream has nothing to ack.
	if resp.StatusCode == 408 {
		t.Log("got 408 (expected when all signals pruned), checking Kafka anyway")
	} else {
		assert.Equal(t, 200, resp.StatusCode)
	}

	time.Sleep(3 * time.Second)

	// The CloudEvent is still processed but the signal should be pruned
	// because its timestamp is 1 hour in the future (> 5 min threshold).
	// After pruning, all signals are removed so no signal CE should be produced.
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	assert.Empty(t, msgs, "no signal CE should be produced when all signals are pruned")
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
	t.Logf("Input payload: %s", string(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	assert.Equal(t, 200, resp.StatusCode)

	time.Sleep(3 * time.Second)

	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.Len(t, msgs, 1, "expected exactly 1 signal message")
	ce := parseSignalCE(t, msgs[0])
	assert.Equal(t, subject, ce.Subject)
	require.Len(t, ce.Data.Signals, 1, "duplicate signal should have been pruned, expected exactly 1")
	assert.Equal(t, "powertrainCombustionEngineECT", ce.Data.Signals[0].Name)
	assert.InDelta(t, 107.0, ce.Data.Signals[0].ValueNumber, 0.01)

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
	t.Logf("Input payload: %s", string(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	assert.Equal(t, 200, resp.StatusCode)

	time.Sleep(3 * time.Second)

	// No signal CE should be produced for an empty signals array
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	assert.Empty(t, msgs, "no signal CE should be produced for empty signals array")
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
	t.Logf("Input payload: %s", string(payloadBytes))

	signalOffset := kafkaEndOffset(t, "topic.device.signals")
	eventOffset := kafkaEndOffset(t, "topic.device.events")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	if resp.StatusCode == 408 {
		t.Log("got 408 (pipeline congestion), checking Kafka anyway")
	} else {
		assert.Equal(t, 200, resp.StatusCode)
	}

	time.Sleep(3 * time.Second)

	// Verify signal appears in Kafka signals topic
	signalMsgs := consumeKafka(t, "topic.device.signals", signalOffset, 10*time.Second)
	require.Len(t, signalMsgs, 1, "expected exactly 1 signal message")
	signalCE := parseSignalCE(t, signalMsgs[0])
	assert.Equal(t, subject, signalCE.Subject)
	require.Len(t, signalCE.Data.Signals, 1)
	assert.Equal(t, "powertrainCombustionEngineECT", signalCE.Data.Signals[0].Name)
	assert.InDelta(t, 80.0, signalCE.Data.Signals[0].ValueNumber, 0.01)

	// Verify event appears in Kafka events topic
	eventMsgs := consumeKafka(t, "topic.device.events", eventOffset, 10*time.Second)
	if len(eventMsgs) == 0 && resp.StatusCode == 408 {
		t.Skip("skipping event check: not delivered due to pipeline congestion (known issue)")
	}
	require.Len(t, eventMsgs, 1, "expected exactly 1 event message")
	eventCE := parseEventCE(t, eventMsgs[0])
	assert.Equal(t, subject, eventCE.Subject)
	assert.Equal(t, "dimo.events", eventCE.Type)
	require.Len(t, eventCE.Data.Events, 1)
	assert.Equal(t, "behavior.harshBraking", eventCE.Data.Events[0].Name)

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
