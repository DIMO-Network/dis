package cloudeventconvert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

//go:generate mockgen -source=./cloudeventconvert.go -destination=./cloudeventconvert_mock_test.go -package=cloudeventconvert

func TestProcessBatch(t *testing.T) {
	timestamp := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		setupMock     func(*MockCloudEventModule)
		expectedMeta  map[string]any
		name          string
		sourceID      string
		inputData     []byte
		msgLen        int
		expectedError bool
	}{
		{
			name:      "successful event conversion",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func(m *MockCloudEventModule) {
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
					Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
					Time:     timestamp,
				}
				event2 := event
				event2.Type = cloudevent.TypeFingerprint
				data := json.RawMessage(`{"key": "value"}`)

				m.EXPECT().CloudEventConvert(gomock.Any(), []byte(`{"test": "data"}`)).Return([]cloudevent.CloudEventHeader{event, event2}, data, nil)
			},
			msgLen:        2,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            cloudevent.TypeStatus,
				cloudEventProducerKey:        "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
				cloudEventSubjectKey:         "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
				processors.MessageContentKey: "dimo_valid_cloudevent",
				CloudEventIndexValueKey: []cloudevent.CloudEventHeader{
					{
						ID:       "33",
						Source:   common.HexToAddress("0x").String(),
						Type:     cloudevent.TypeStatus,
						Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
						Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
						Time:     timestamp,
					},
					{
						ID:       "33",
						Source:   common.HexToAddress("0x").String(),
						Type:     cloudevent.TypeFingerprint,
						Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
						Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
						Time:     timestamp,
					},
				},
			},
		},
		{
			name:      "Future timestamp error",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func(m *MockCloudEventModule) {
				event := cloudevent.CloudEventHeader{
					Type:     fmt.Sprintf("%s, %s", cloudevent.TypeStatus, cloudevent.TypeFingerprint),
					Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
					Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
					Time:     time.Now().Add(time.Hour),
				}
				data := json.RawMessage(`{"key": "value"}`)

				m.EXPECT().CloudEventConvert(gomock.Any(), []byte(`{"test": "data"}`)).Return([]cloudevent.CloudEventHeader{event}, data, nil)
			},
			msgLen:        1,
			expectedError: true,
		},
		{
			name:      "missing source ID",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "", // Empty source
			setupMock: func(m *MockCloudEventModule) {
				// No mock expectations since it should fail before conversion
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:      "conversion error",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func(m *MockCloudEventModule) {
				m.EXPECT().CloudEventConvert(gomock.Any(), []byte(`{"test": "data"}`)).Return(nil, nil, errors.New("conversion failed"))
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:      "invalid cloud event format",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func(m *MockCloudEventModule) {
				m.EXPECT().CloudEventConvert(gomock.Any(), []byte(`{"test": "data"}`)).Return(nil, []byte(`invalid json`), nil)
			},
			msgLen:        1,
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
			require.Len(t, result[0], tt.msgLen, "unexpected number of messages in batch")

			outMsg := result[0][0]
			if tt.expectedError {
				require.NotNil(t, outMsg.GetError(), "expected error but got none")
			} else {
				require.Nil(t, outMsg.GetError(), "unexpected error: %v", outMsg.GetError())

				// Validate metadata
				for key, expectedValue := range tt.expectedMeta {
					actualValue, exists := outMsg.MetaGetMut(key)
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
