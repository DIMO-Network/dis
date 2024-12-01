package cloudeventconvert

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

//go:generate mockgen -source=./cloudeventconvert.go -destination=./cloudeventconvert_mock_test.go -package=cloudeventconvert

func TestProcessBatch(t *testing.T) {
	tests := []struct {
		setupMock     func(*MockCloudEventModule)
		expectedMeta  map[string]string
		name          string
		sourceID      string
		inputData     []byte
		expectedError bool
	}{
		{
			name:      "successful event conversion",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func(m *MockCloudEventModule) {
				event := cloudevent.CloudEvent[json.RawMessage]{
					CloudEventHeader: cloudevent.CloudEventHeader{
						Type:     "test.event",
						Producer: "test-producer",
						Subject:  "test-subject",
						Source:   "test-source",
						Time:     time.Now().UTC(),
					},
					Data: json.RawMessage(`{"key": "value"}`),
				}
				eventBytes, _ := json.Marshal(event)
				m.EXPECT().CloudEventConvert(gomock.Any(), []byte(`{"test": "data"}`)).Return([][]byte{eventBytes}, nil)
			},
			expectedError: false,
			expectedMeta: map[string]string{
				cloudEventTypeKey:     "test.event",
				cloudEventProducerKey: "test-producer",
				cloudEventSubjectKey:  "test-subject",
				CloudEventValidKey:    "false",
			},
		},
		{
			name:      "missing source ID",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "", // Empty source
			setupMock: func(m *MockCloudEventModule) {
				// No mock expectations since it should fail before conversion
			},
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:      "conversion error",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func(m *MockCloudEventModule) {
				m.EXPECT().CloudEventConvert(gomock.Any(), []byte(`{"test": "data"}`)).Return(nil, errors.New("conversion failed"))
			},
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:      "invalid cloud event format",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func(m *MockCloudEventModule) {
				m.EXPECT().CloudEventConvert(gomock.Any(), []byte(`{"test": "data"}`)).Return([][]byte{[]byte(`invalid json`)}, nil)
			},
			expectedError: true,
			expectedMeta:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup controller and mock
			ctrl := gomock.NewController(t)

			mockModule := NewMockCloudEventModule(ctrl)
			tt.setupMock(mockModule)

			processor := &cloudeventProcessor{
				cloudEventModule: mockModule,
				logger:           nil,
			}

			msg := service.NewMessage(tt.inputData)
			if tt.sourceID != "" {
				msg.MetaSet(httpinputserver.DIMOConnectionIdKey, tt.sourceID)
			}

			result, err := processor.ProcessBatch(context.Background(), service.MessageBatch{msg})
			require.NoError(t, err, "unexpected error: %v", err)

			require.NotNil(t, result)
			require.Len(t, result, 1, "processor should always return 1 batch")
			require.Len(t, result[0], 1, "processor should always return 1 message")

			outMsg := result[0][0]
			if tt.expectedError {
				require.NotNil(t, outMsg.GetError(), "expected error but got none")
			} else {
				require.Nil(t, outMsg.GetError(), "unexpected error: %v", outMsg.GetError())

				// Validate metadata
				for key, expectedValue := range tt.expectedMeta {
					actualValue, exists := outMsg.MetaGet(key)
					assert.True(t, exists, "metadata key %s not found", key)
					assert.Equal(t, expectedValue, actualValue, "unexpected value for metadata key %s", key)
				}

				// Check required metadata fields
				_, exists := outMsg.MetaGet(cloudEventIndexKey)
				assert.True(t, exists, "index metadata not found")
			}
		})
	}
}
