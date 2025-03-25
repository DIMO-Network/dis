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
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCloudEventModule struct {
	hdrs []cloudevent.CloudEventHeader
	data []byte
	err  error
}

func (m *mockCloudEventModule) CloudEventConvert(ctx context.Context, data []byte) ([]cloudevent.CloudEventHeader, []byte, error) {
	return m.hdrs, m.data, m.err
}

func TestProcessBatch(t *testing.T) {
	timestamp := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		setupMock      func() *mockCloudEventModule
		expectedMeta   map[string]any
		name           string
		sourceID       string
		messageContent string
		extras         map[string]any
		inputData      []byte
		msgLen         int
		expectedError  bool
	}{
		{
			name:           "successful attestation",
			inputData:      []byte(`{"extras": {"signature": "0x8341fcddc88c8def986943b2253246a38a13593fd5bc3c379f98a8c4730207c349ee3173c130417e1864ccbc997f3585f54cc9c357fe1f2d582b25a2f369a3e11c", "data": "0xb61d27f60000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000000"}}`),
			sourceID:       common.HexToAddress("0xD3Cb8baE2C2Ee0e8333D1c0A8E0179a0D1A3922e").String(),
			messageContent: "dimo_content_attestation",
			extras: map[string]any{
				"signature": "0x8341fcddc88c8def986943b2253246a38a13593fd5bc3c379f98a8c4730207c349ee3173c130417e1864ccbc997f3585f54cc9c357fe1f2d582b25a2f369a3e11c",
			},
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     "dimo.attestation", // TODO(ae): move to model garage
					Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
					Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
					Time:     timestamp,
				}
				data := json.RawMessage(`{"key": "value"}`)

				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: data,
					err:  nil,
				}
			},
			msgLen:        1,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            "dimo.attestation", //TODO(ae), update w const
				cloudEventProducerKey:        "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
				cloudEventSubjectKey:         "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
				processors.MessageContentKey: "dimo_valid_cloudevent",
				CloudEventIndexValueKey: []cloudevent.CloudEventHeader{
					{
						ID:       "33",
						Source:   common.HexToAddress("0xD3Cb8baE2C2Ee0e8333D1c0A8E0179a0D1A3922e").String(),
						Type:     "dimo.attestation", //TODO(ae), update w const
						Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
						Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
						Time:     timestamp,
					},
				},
			},
		},
		{
			name:      "successful event conversion",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
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

				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event, event2},
					data: data,
					err:  nil,
				}
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
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					Type:     fmt.Sprintf("%s, %s", cloudevent.TypeStatus, cloudevent.TypeFingerprint),
					Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
					Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
					Time:     time.Now().Add(time.Hour),
				}
				data := json.RawMessage(`{"key": "value"}`)

				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: data,
					err:  nil,
				}
			},
			msgLen:        1,
			expectedError: false,
		},
		{
			name:      "missing source ID",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "", // Empty source
			setupMock: func() *mockCloudEventModule {
				// Return an empty mock as it should fail before conversion
				return &mockCloudEventModule{}
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:      "conversion error",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{
					hdrs: nil,
					data: nil,
					err:  errors.New("conversion failed"),
				}
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:      "invalid cloud event format",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{
					hdrs: nil,
					data: json.RawMessage(`invalid json`),
					err:  nil,
				}
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock
			mockModule := tt.setupMock()
			modules.CloudEventRegistry.Override(tt.sourceID, mockModule)

			processor := &cloudeventProcessor{
				logger: nil,
			}

			msg := service.NewMessage(tt.inputData)

			if tt.sourceID != "" {
				msg.MetaSet(httpinputserver.DIMOCloudEventSource, tt.sourceID)
			}
			if tt.messageContent != "" {
				msg.MetaSet("dimo_message_content", tt.messageContent)
			}
			if len(tt.extras) != 0 {
				msg.MetaSetMut("extras", tt.extras)
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
