//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultModuleEvents(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:888"

	payload := map[string]any{
		"id":             "test-event-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.event",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 888,
		"data": map[string]any{
			"events": []map[string]any{
				{
					"name":      "behavior.harshBraking",
					"timestamp": "2024-04-18T17:20:46.436008782Z",
					"metadata":  `{"ignition":1,"speed":10}`,
				},
				{
					"name":       "behavior.harshAcceleration",
					"timestamp":  "2024-04-18T17:35:46.436008782Z",
					"durationNs": 15000000000,
					"metadata":   "",
				},
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload: %s", string(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.events")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	assert.Equal(t, 200, resp.StatusCode)

	time.Sleep(5 * time.Second)

	// Check Kafka events topic
	msgs := consumeKafka(t, "topic.device.events", startOffset, 10*time.Second)
	require.Len(t, msgs, 1, "expected exactly 1 event message")
	ce := parseEventCE(t, msgs[0])
	assert.Equal(t, subject, ce.Subject)
	assert.Equal(t, "1.0", ce.SpecVersion)
	assert.Equal(t, "dimo.events", ce.Type)
	assert.Equal(t, testSourceAddress, ce.Source)
	require.Len(t, ce.Data.Events, 2)
	assert.Equal(t, "behavior.harshBraking", ce.Data.Events[0].Name)
	assert.Equal(t, "behavior.harshAcceleration", ce.Data.Events[1].Name)

	// Check ClickHouse event table — should have exactly 2 event rows
	eventRows := queryEvents(t, subject)
	require.Len(t, eventRows, 2, "expected exactly 2 event rows in ClickHouse for 2 input events")

	// Verify event names (queryEvents orders by name)
	eventNames := make([]string, len(eventRows))
	for i, r := range eventRows {
		eventNames[i] = r.Name
		assert.Equal(t, testSourceAddress, r.Source, "source mismatch for event %s", r.Name)
		assert.Equal(t, subject, r.Producer, "producer mismatch for event %s", r.Name)
		assert.NotEmpty(t, r.CloudEventID, "cloud_event_id should be set for event %s", r.Name)
	}
	assert.Contains(t, eventNames, "behavior.harshBraking")
	assert.Contains(t, eventNames, "behavior.harshAcceleration")

	// Verify metadata on harshBraking event
	for _, r := range eventRows {
		if r.Name == "behavior.harshBraking" {
			assert.NotEmpty(t, r.Metadata, "harshBraking should have metadata")
		}
		if r.Name == "behavior.harshAcceleration" {
			assert.Equal(t, uint64(15000000000), r.DurationNs, "harshAcceleration should have 15s duration")
		}
	}
}
