package eventconvert

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockEventModule implements the modules interface for testing
type mockEventModule struct {
	events []vss.Event
	err    error
}

func (m *mockEventModule) EventConvert(ctx context.Context, rawEvent cloudevent.RawEvent) ([]vss.Event, error) {
	if m.err != nil {
		return nil, &convert.ConversionError{
			Errors:        []error{m.err},
			DecodedEvents: m.events,
		}
	}
	return m.events, m.err
}

func TestProcessBatch_SuccessfulConversion(t *testing.T) {
	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	// Setup mock
	mockModule := &mockEventModule{
		events: []vss.Event{
			{
				Name:       "tripStart",
				Timestamp:  timestamp,
				Subject:    "did:erc721:1:0x123:456",
				Source:     "test-source",
				Metadata:   `{"confidence": 0.95}`,
				DurationNs: 0,
			},
		},
		err: nil,
	}

	originalModule, _ := modules.EventRegistry.Get("")
	defer func() {
		if originalModule != nil {
			modules.EventRegistry.Override("", originalModule)
		}
	}()
	modules.EventRegistry.Override("", mockModule)

	processor := &eventsProcessor{logger: nil}
	msg := createVehicleEventMessage(t, timestamp)

	batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, batches, 1)
	batch := batches[0]
	require.Len(t, batch, 2) // original + converted events

	// Check that the second message contains converted events
	eventsMsg := batch[1]
	content, exists := eventsMsg.MetaGet(processors.MessageContentKey)
	assert.True(t, exists)
	assert.Equal(t, eventValidContentType, content)

	// Check structured data contains events
	structured, err := eventsMsg.AsStructured()
	require.NoError(t, err)
	events, ok := structured.([]vss.Event)
	require.True(t, ok)
	require.Len(t, events, 1)
	assert.Equal(t, "tripStart", events[0].Name)
	assert.Equal(t, "test-source", events[0].Source)
	assert.Equal(t, "test-producer", events[0].Producer)
	assert.Equal(t, "test-id", events[0].CloudEventID)
	assert.Equal(t, "did:erc721:1:0x123:456", events[0].Subject)
	assert.Equal(t, timestamp, events[0].Timestamp)
	assert.Equal(t, uint64(0), events[0].DurationNs)
	assert.Equal(t, "{\"confidence\": 0.95}", events[0].Metadata)
}

func TestProcessBatch_WithPartialError(t *testing.T) {
	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	mockModule := &mockEventModule{
		events: []vss.Event{{Name: "tripStart", Timestamp: timestamp, DurationNs: 0}},
		err:    errors.New("partial conversion error"),
	}

	originalModule, _ := modules.EventRegistry.Get("")
	defer func() {
		if originalModule != nil {
			modules.EventRegistry.Override("", originalModule)
		}
	}()
	modules.EventRegistry.Override("", mockModule)

	processor := &eventsProcessor{logger: nil}
	msg := createVehicleEventMessage(t, timestamp)

	batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, batches, 1)
	batch := batches[0]
	require.Len(t, batch, 3) // original + error + events

	// Check error message
	errorMsg := batch[1]
	require.NotNil(t, errorMsg.GetError())

	// Check events still get processed
	eventsMsg := batch[2]
	structured, err := eventsMsg.AsStructured()
	require.NoError(t, err)
	events, ok := structured.([]vss.Event)
	require.True(t, ok)
	require.Len(t, events, 1)
}

func TestProcessBatch_NonVehicleEventIgnored(t *testing.T) {
	mockModule := &mockEventModule{}

	originalModule, _ := modules.EventRegistry.Get("")
	defer func() {
		if originalModule != nil {
			modules.EventRegistry.Override("", originalModule)
		}
	}()
	modules.EventRegistry.Override("", mockModule)

	processor := &eventsProcessor{logger: nil}
	msg := createNonVehicleEventMessage(t)

	batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, batches, 1)
	batch := batches[0]
	require.Len(t, batch, 1) // only original message

	// Should be the original message unchanged
	originalMsg := batch[0]
	require.Nil(t, originalMsg.GetError())
}

func TestProcessBatch_EmptyEventsArray(t *testing.T) {
	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	mockModule := &mockEventModule{
		events: []vss.Event{}, // empty array
		err:    nil,
	}

	originalModule, _ := modules.EventRegistry.Get("")
	defer func() {
		if originalModule != nil {
			modules.EventRegistry.Override("", originalModule)
		}
	}()
	modules.EventRegistry.Override("", mockModule)

	processor := &eventsProcessor{logger: nil}
	msg := createVehicleEventMessage(t, timestamp)

	batches, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)

	require.Len(t, batches, 1)
	batch := batches[0]
	require.Len(t, batch, 1) // only original message (no events message when empty)
}

// Helper functions for creating test messages

func createVehicleEventMessage(t *testing.T, timestamp time.Time) *service.Message {
	t.Helper()

	event := &cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			Type:     cloudevent.TypeEvent,
			Subject:  "did:erc721:1:0x123:456",
			Source:   "test-source",
			Producer: "test-producer",
			ID:       "test-id",
			Time:     timestamp,
		},
		Data: json.RawMessage(`{
			"events": [
				{
					"name": "tripStart",
					"timestamp": "2023-01-01T12:00:00Z",
					"metadata": "{\"confidence\": 0.95}"
				}
			]
		}`),
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(event)
	return msg
}

func createNonVehicleEventMessage(t *testing.T) *service.Message {
	t.Helper()

	event := &cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			Type:    cloudevent.TypeStatus,
			Subject: "did:erc721:1:0x123:456",
			Source:  "test-source",
			ID:      "test-id",
		},
		Data: json.RawMessage(`{"status": "connected"}`),
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(event)
	return msg
}
