package fingerprintvalidate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockConvertToFingerprint struct{}

// Mock for modules.FingerprintConvert
func (mockConvertToFingerprint) FingerprintConvert(ctx context.Context, event cloudevent.RawEvent) (cloudevent.Fingerprint, error) {
	var fp cloudevent.Fingerprint
	err := json.Unmarshal(event.Data, &fp)
	if err != nil {
		return cloudevent.Fingerprint{}, err
	}
	return fp, nil
}

func TestProcessBatch(t *testing.T) {
	// Override the ConvertToFingerprint function for testing
	oldDefault, _ := modules.FingerprintRegistry.Get("")
	defer func() {
		modules.FingerprintRegistry.Override("", oldDefault)
	}()
	modules.FingerprintRegistry.Override("", mockConvertToFingerprint{})

	proc := processor{}

	tests := []struct {
		name            string
		messages        []*service.Message
		expectedBatches int
		expectedErrors  int
	}{
		{
			name: "Valid VIN",
			messages: []*service.Message{
				createFingerprintMessage(t, "1HGCM82633A123456"),
			},
			expectedBatches: 1,
			expectedErrors:  0,
		},
		{
			name: "Invalid VIN - too short",
			messages: []*service.Message{
				createFingerprintMessage(t, "1HGCM123"),
			},
			expectedBatches: 1,
			expectedErrors:  1,
		},
		{
			name: "Invalid VIN - contains invalid characters",
			messages: []*service.Message{
				createFingerprintMessage(t, "1HGCM82633A12345I"), // 'I' is not a valid character
			},
			expectedBatches: 1,
			expectedErrors:  1,
		},
		{
			name: "Invalid VIN - contains invalid characters",
			messages: []*service.Message{
				createFingerprintMessage(t, "1HGCM82633A12345Q"), // 'Q' is not a valid character
			},
			expectedBatches: 1,
			expectedErrors:  1,
		},
		{
			name: "Invalid VIN - contains invalid characters",
			messages: []*service.Message{
				createFingerprintMessage(t, "AAAAAAAAAAAAAAAAQ"), // 'Q' is not a valid character
			},
			expectedBatches: 1,
			expectedErrors:  1,
		},

		{
			name: "Valid Japan Chassis VIN",
			messages: []*service.Message{
				createFingerprintMessage(t, "SNT33-042261"), // 'Q' is not a valid character
			},
			expectedBatches: 1,
			expectedErrors:  0,
		},
		{
			name: "Non-fingerprint event",
			messages: []*service.Message{
				createNonFingerprintMessage(t),
			},
			expectedBatches: 1,
			expectedErrors:  0,
		},
		{
			name: "Invalid JSON message",
			messages: []*service.Message{
				service.NewMessage([]byte(`{"this is not valid JSON`)),
			},
			expectedBatches: 1,
			expectedErrors:  1,
		},
		{
			name: "Multiple messages - mixed types",
			messages: []*service.Message{
				createFingerprintMessage(t, "1HGCM82633A123456"),
				createNonFingerprintMessage(t),
				createFingerprintMessage(t, "ABCDE12345FGHJ789"),
				createFingerprintMessage(t, "INVALID-VIN-HERE!"),
			},
			expectedBatches: 4,
			expectedErrors:  1,
		},
	}

	// Run the tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := service.MessageBatch(tt.messages)

			batches, err := proc.ProcessBatch(context.Background(), batch)
			assert.NoError(t, err)

			// Check that we have the expected number of batches
			assert.Equal(t, tt.expectedBatches, len(batches))

			// Count errors
			errorCount := 0
			for _, b := range batches {
				for _, m := range b {
					if m.GetError() != nil {
						errorCount++
					}
				}
			}
			assert.Equal(t, tt.expectedErrors, errorCount)
		})
	}
}

func createFingerprintMessage(t *testing.T, vin string) *service.Message {
	t.Helper()
	var fingerprintEvent cloudevent.FingerprintEvent
	fingerprintEvent.Type = cloudevent.TypeFingerprint
	fingerprintEvent.Data.VIN = vin
	data, err := json.Marshal(fingerprintEvent.Data)
	require.NoError(t, err)
	return createRawMsg(fingerprintEvent.CloudEventHeader, data)
}

func createNonFingerprintMessage(t *testing.T) *service.Message {
	t.Helper()
	var hdr cloudevent.CloudEventHeader
	hdr.Type = cloudevent.TypeStatus
	return createRawMsg(hdr, []byte(`{"key": "value"}`))
}

func createRawMsg(hdr cloudevent.CloudEventHeader, data []byte) *service.Message {
	event := &cloudevent.RawEvent{
		CloudEventHeader: hdr,
		Data:             data,
	}
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(event)
	return msg
}
