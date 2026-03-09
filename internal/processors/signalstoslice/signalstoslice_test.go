package signalstoslice

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

func TestProcess_MultipleSignals(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	signalCE := vss.SignalCloudEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          "ce-123",
			Source:      "0xABCD",
			Subject:     "did:erc721:1:0xVehicle:42",
			Producer:    "did:nft:137:0xProducer:99",
			Time:        now,
			Type:        "dimo.status",
		},
		Data: vss.SignalsPayload{
			Signals: []vss.SignalData{
				{
					Timestamp:    now,
					Name:         "speed",
					ValueNumber:  65.5,
					CloudEventID: "ce-123",
				},
				{
					Timestamp:    now,
					Name:         "odometer",
					ValueNumber:  12345,
					CloudEventID: "ce-123",
				},
				{
					Timestamp:   now,
					Name:        "currentLocationCoordinates",
					CloudEventID: "ce-123",
					ValueLocation: vss.Location{
						Latitude:  37.7749,
						Longitude: -122.4194,
						HDOP:      1.2,
						Heading:   180.5,
					},
				},
			},
		},
	}

	payload, err := json.Marshal(signalCE)
	require.NoError(t, err)

	msg := service.NewMessage(payload)
	proc := newSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batch, 3)

	// Verify each output message is a slice matching SignalToSlice order.
	// Order: Subject, Timestamp, Name, Source, Producer, CloudEventID, ValueNumber, ValueString, ValueLocation
	for i, m := range batch {
		structured, sErr := m.AsStructured()
		require.NoError(t, sErr, "message %d", i)
		slice, ok := structured.([]any)
		require.True(t, ok, "message %d should be []any", i)
		require.Len(t, slice, 9, "message %d should have 9 columns", i)

		// All signals share the same header fields from the envelope.
		assert.Equal(t, "did:erc721:1:0xVehicle:42", slice[0], "subject")
		assert.Equal(t, "0xABCD", slice[3], "source")
		assert.Equal(t, "did:nft:137:0xProducer:99", slice[4], "producer")
		assert.Equal(t, "ce-123", slice[5], "cloudEventID")
	}

	// First signal: speed
	s0, _ := batch[0].AsStructured()
	slice0 := s0.([]any)
	assert.Equal(t, "speed", slice0[2])
	assert.Equal(t, float64(65.5), slice0[6])

	// Second signal: odometer
	s1, _ := batch[1].AsStructured()
	slice1 := s1.([]any)
	assert.Equal(t, "odometer", slice1[2])
	assert.Equal(t, float64(12345), slice1[6])

	// Third signal: location
	s2, _ := batch[2].AsStructured()
	slice2 := s2.([]any)
	assert.Equal(t, "currentLocationCoordinates", slice2[2])
	loc, ok := slice2[8].(vss.Location)
	require.True(t, ok)
	assert.Equal(t, 37.7749, loc.Latitude)
	assert.Equal(t, -122.4194, loc.Longitude)
}

func TestProcess_SingleSignal(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	signalCE := vss.SignalCloudEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:      "single-id",
			Source:  "src",
			Subject: "subj",
		},
		Data: vss.SignalsPayload{
			Signals: []vss.SignalData{
				{Timestamp: now, Name: "isIgnitionOn", ValueString: "true"},
			},
		},
	}

	payload, err := json.Marshal(signalCE)
	require.NoError(t, err)

	msg := service.NewMessage(payload)
	proc := newSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	structured, err := batch[0].AsStructured()
	require.NoError(t, err)
	slice := structured.([]any)
	assert.Equal(t, "isIgnitionOn", slice[2])
	assert.Equal(t, "true", slice[7])
}

func TestProcess_EmptySignals(t *testing.T) {
	t.Parallel()

	signalCE := vss.SignalCloudEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{ID: "empty"},
		Data:             vss.SignalsPayload{Signals: []vss.SignalData{}},
	}

	payload, err := json.Marshal(signalCE)
	require.NoError(t, err)

	msg := service.NewMessage(payload)
	proc := newSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	assert.Nil(t, batch, "empty signals should return nil batch")
}

func TestProcess_InvalidJSON(t *testing.T) {
	t.Parallel()

	msg := service.NewMessage([]byte(`not valid json`))
	proc := newSliceProcessor(nil)

	batch, err := proc.Process(context.Background(), msg)
	assert.Error(t, err)
	assert.Nil(t, batch)
}
