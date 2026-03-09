package eventstoslice

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcess_MultipleEvents(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	eventCE := vss.EventCloudEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          "ce-evt-123",
			Source:      "0xABCD",
			Subject:     "did:erc721:1:0xVehicle:42",
			Producer:    "did:nft:137:0xProducer:99",
			Time:        now,
			Type:        "dimo.event",
			DataVersion: "1.0",
			Tags:        []string{"behavior"},
		},
		Data: vss.EventsPayload{
			Events: []vss.EventData{
				{
					Name:         "behavior.harshBraking",
					Timestamp:    now,
					DurationNs:   0,
					Metadata:     `{"counterValue":3}`,
					CloudEventID: "ce-evt-123",
				},
				{
					Name:         "behavior.extremeBraking",
					Timestamp:    now,
					DurationNs:   5000000000,
					Metadata:     `{"counterValue":1}`,
					CloudEventID: "ce-evt-123",
				},
			},
		},
	}

	payload, err := json.Marshal(eventCE)
	require.NoError(t, err)

	msg := service.NewMessage(payload)
	proc := newEventSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batch, 2)

	// Verify each output message is a slice matching EventToSlice order.
	// Order: Subject, Source, Producer, ID, Type, DataVersion, Name, Timestamp, DurationNs, Metadata, Tags
	for i, m := range batch {
		structured, sErr := m.AsStructured()
		require.NoError(t, sErr, "message %d", i)
		slice, ok := structured.([]any)
		require.True(t, ok, "message %d should be []any", i)
		require.Len(t, slice, 11, "message %d should have 11 columns", i)

		// All events share header fields from the envelope.
		assert.Equal(t, "did:erc721:1:0xVehicle:42", slice[0], "subject")
		assert.Equal(t, "0xABCD", slice[1], "source")
		assert.Equal(t, "did:nft:137:0xProducer:99", slice[2], "producer")
		assert.Equal(t, "ce-evt-123", slice[3], "cloudEventID")
		assert.Equal(t, "dimo.event", slice[4], "type")
		assert.Equal(t, "1.0", slice[5], "dataVersion")
	}

	// First event: harshBraking
	s0, _ := batch[0].AsStructured()
	slice0 := s0.([]any)
	assert.Equal(t, "behavior.harshBraking", slice0[6])
	assert.Equal(t, now, slice0[7])
	assert.Equal(t, uint64(0), slice0[8])
	assert.Equal(t, `{"counterValue":3}`, slice0[9])

	// Second event: extremeBraking
	s1, _ := batch[1].AsStructured()
	slice1 := s1.([]any)
	assert.Equal(t, "behavior.extremeBraking", slice1[6])
	assert.Equal(t, uint64(5000000000), slice1[8])
	assert.Equal(t, `{"counterValue":1}`, slice1[9])
}

func TestProcess_SingleEvent(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	eventCE := vss.EventCloudEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:          "single-evt",
			Source:      "src",
			Subject:     "subj",
			Type:        "dimo.event",
			DataVersion: "2.0",
		},
		Data: vss.EventsPayload{
			Events: []vss.EventData{
				{
					Name:      "safety.collision",
					Timestamp: now,
					Metadata:  `{"severity":"high"}`,
				},
			},
		},
	}

	payload, err := json.Marshal(eventCE)
	require.NoError(t, err)

	msg := service.NewMessage(payload)
	proc := newEventSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	structured, err := batch[0].AsStructured()
	require.NoError(t, err)
	slice := structured.([]any)
	assert.Equal(t, "safety.collision", slice[6])
	assert.Equal(t, "2.0", slice[5])
	assert.Equal(t, `{"severity":"high"}`, slice[9])
}

func TestProcess_EmptyEvents(t *testing.T) {
	t.Parallel()

	eventCE := vss.EventCloudEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{ID: "empty"},
		Data:             vss.EventsPayload{Events: []vss.EventData{}},
	}

	payload, err := json.Marshal(eventCE)
	require.NoError(t, err)

	msg := service.NewMessage(payload)
	proc := newEventSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	assert.Nil(t, batch, "empty events should return nil batch")
}

func TestProcess_InvalidJSON(t *testing.T) {
	t.Parallel()

	msg := service.NewMessage([]byte(`{broken`))
	proc := newEventSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	assert.Error(t, err)
	assert.Nil(t, batch)
}
