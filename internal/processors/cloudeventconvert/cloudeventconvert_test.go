package cloudeventconvert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
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
	now := time.Now().UTC()
	attestationTimestamp := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)
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
			inputData:      []byte(fmt.Sprintf(`{"id":"848","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"did:ethr:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:848","time":"%s","type":"dimo.attestation","signature":"0xa9d9d02cde3c18def3836e039b967fb0363f09f8dcf2a5ca07831443b7936e76776051405f7fa44b4864d9cf013730b1c6d63871636ac872c1b031010a4621281b","data":{"goodTires":true}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "848",
					Type:     cloudevent.TypeAttestation,
					Producer: "did:ethr:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
					Subject:  "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:848",
					Time:     attestationTimestamp,
				}

				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					err:  nil,
				}
			},
			msgLen:        1,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            cloudevent.TypeAttestation,
				cloudEventProducerKey:        "did:ethr:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
				cloudEventSubjectKey:         "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:848",
				processors.MessageContentKey: "dimo_valid_cloudevent",
				CloudEventIndexValueKey: []cloudevent.CloudEventHeader{
					{
						ID:       "848",
						Source:   common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
						Type:     cloudevent.TypeAttestation,
						Producer: "did:ethr:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
						Subject:  "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:848",
						Time:     attestationTimestamp,
						Extras: map[string]any{
							"signature": "0xa9d9d02cde3c18def3836e039b967fb0363f09f8dcf2a5ca07831443b7936e76776051405f7fa44b4864d9cf013730b1c6d63871636ac872c1b031010a4621281b",
						},
						DataContentType: "application/json",
						SpecVersion:     "1.0",
					},
				},
			},
		},
		{
			name:           "successful event conversion",
			inputData:      []byte(`{"test": "data"}`),
			sourceID:       common.HexToAddress("0x").String(),
			messageContent: httpinputserver.ConnectionContent,
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
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
				cloudEventProducerKey:        "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
				cloudEventSubjectKey:         "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
				processors.MessageContentKey: "dimo_valid_cloudevent",
				CloudEventIndexValueKey: []cloudevent.CloudEventHeader{
					{
						ID:              "33",
						Source:          common.HexToAddress("0x").String(),
						Type:            cloudevent.TypeStatus,
						Producer:        "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
						Subject:         "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
						Time:            timestamp,
						SpecVersion:     "1.0",
						DataContentType: "application/json",
					},
					{
						ID:              "33",
						Source:          common.HexToAddress("0x").String(),
						Type:            cloudevent.TypeFingerprint,
						Producer:        "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
						Subject:         "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
						Time:            timestamp,
						SpecVersion:     "1.0",
						DataContentType: "application/json",
					},
				},
			},
		},
		{
			name:           "successful event conversion with legacy NFT DID",
			inputData:      []byte(`{"test": "data"}`),
			sourceID:       common.HexToAddress("0x").String(),
			messageContent: httpinputserver.ConnectionContent,
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
				cloudEventProducerKey:        "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
				cloudEventSubjectKey:         "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
				processors.MessageContentKey: "dimo_valid_cloudevent",
				CloudEventIndexValueKey: []cloudevent.CloudEventHeader{
					{
						ID:              "33",
						Source:          common.HexToAddress("0x").String(),
						Type:            cloudevent.TypeStatus,
						Producer:        "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
						Subject:         "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
						Time:            timestamp,
						SpecVersion:     "1.0",
						DataContentType: "application/json",
					},
					{
						ID:              "33",
						Source:          common.HexToAddress("0x").String(),
						Type:            cloudevent.TypeFingerprint,
						Producer:        "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
						Subject:         "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
						Time:            timestamp,
						SpecVersion:     "1.0",
						DataContentType: "application/json",
					},
				},
			},
		},
		{
			name:           "Future timestamp error",
			inputData:      []byte(`{"test": "data"}`),
			sourceID:       common.HexToAddress("0x").String(),
			messageContent: httpinputserver.ConnectionContent,
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
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
				msg.MetaSet(processors.MessageContentKey, tt.messageContent)
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
					require.True(t, exists, "metadata key %s not found", key)
					assert.Equal(t, expectedValue, actualValue, "unexpected value for metadata key %s", key)
				}

				// Check required metadata fields
				_, exists := outMsg.MetaGet(cloudEventIndexKey)
				assert.True(t, exists, "index metadata not found")
			}
		})
	}
}
