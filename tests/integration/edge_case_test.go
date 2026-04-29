//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFutureTimestampSignalRejected(t *testing.T) {
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
	require.NotEqual(t, 200, resp.StatusCode, "future-timestamp signal should be rejected")

	time.Sleep(750 * time.Millisecond)

	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.Empty(t, msgs, "no signal CE should be produced when the input is rejected")
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
	require.Equal(t, 200, resp.StatusCode)

	time.Sleep(750 * time.Millisecond)

	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.Len(t, msgs, 1, "expected exactly 1 signal message")
	ce := parseSignalCE(t, msgs[0])
	require.Equal(t, subject, ce.Subject)
	require.Len(t, ce.Data.Signals, 1, "duplicate signal should have been pruned, expected exactly 1")
	require.Equal(t, "powertrainCombustionEngineECT", ce.Data.Signals[0].Name)
	require.InDelta(t, 107.0, ce.Data.Signals[0].ValueNumber, 0.01)

	// ClickHouse should also have exactly 1 row (deduplicated)
	rows := querySignals(t, subject)
	require.Len(t, rows, 1, "ClickHouse should have exactly 1 signal row after deduplication")
	require.Equal(t, "powertrainCombustionEngineECT", rows[0].Name)
	require.InDelta(t, 107.0, rows[0].ValueNumber, 0.01)
}

func TestEmptySignalsArrayRejected(t *testing.T) {
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
	require.NotEqual(t, 200, resp.StatusCode, "empty signals array should be rejected")

	time.Sleep(750 * time.Millisecond)

	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.Empty(t, msgs, "no signal CE should be produced for empty signals array")
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
	require.Equal(t, 200, resp.StatusCode)

	time.Sleep(750 * time.Millisecond)

	// Verify signal appears in Kafka signals topic
	signalMsgs := consumeKafka(t, "topic.device.signals", signalOffset, 10*time.Second)
	require.Len(t, signalMsgs, 1, "expected exactly 1 signal message")
	signalCE := parseSignalCE(t, signalMsgs[0])
	require.Equal(t, subject, signalCE.Subject)
	require.Len(t, signalCE.Data.Signals, 1)
	require.Equal(t, "powertrainCombustionEngineECT", signalCE.Data.Signals[0].Name)
	require.InDelta(t, 80.0, signalCE.Data.Signals[0].ValueNumber, 0.01)

	// Verify event appears in Kafka events topic
	eventMsgs := consumeKafka(t, "topic.device.events", eventOffset, 10*time.Second)
	require.Len(t, eventMsgs, 1, "expected exactly 1 event message")
	eventCE := parseEventCE(t, eventMsgs[0])
	require.Equal(t, subject, eventCE.Subject)
	require.Equal(t, "dimo.events", eventCE.Type)
	require.Len(t, eventCE.Data.Events, 1)
	require.Equal(t, "behavior.harshBraking", eventCE.Data.Events[0].Name)

	// ── ClickHouse signal table — exactly 1 signal row ──────────
	signalRows := querySignals(t, subject)
	require.Len(t, signalRows, 1, "expected exactly 1 signal row in ClickHouse for mixed payload")
	require.Equal(t, "powertrainCombustionEngineECT", signalRows[0].Name)
	require.InDelta(t, 80.0, signalRows[0].ValueNumber, 0.01)
	require.Equal(t, testSourceAddress, signalRows[0].Source)

	// ── ClickHouse event table — exactly 1 event row ────────────
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 1, "expected exactly 1 event row in ClickHouse for mixed payload")
	require.Equal(t, "behavior.harshBraking", eventRows[0].Name)
	require.Equal(t, testSourceAddress, eventRows[0].Source)
}
