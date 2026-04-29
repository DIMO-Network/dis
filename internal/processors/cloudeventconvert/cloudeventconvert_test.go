package cloudeventconvert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	"github.com/DIMO-Network/dis/internal/processors/rawparquet"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
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
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "unique-attestation-id-1",
					Type:     cloudevent.TypeAttestation,
					Producer: "did:ethr:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
					Subject:  "did:erc721:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b:1005",
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
				cloudEventProducerKey:        "0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
				cloudEventSubjectKey:         "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005",
				processors.MessageContentKey: "dimo_valid_cloudevent",
			},
		},
		{
			name:           "successful attestation with ethr DID",
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "unique-attestation-id-1",
					Type:     cloudevent.TypeAttestation,
					Producer: "did:ethr:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
					Subject:  "did:ethr:80002:0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
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
				cloudEventProducerKey:        "0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
				cloudEventSubjectKey:         "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005",
				processors.MessageContentKey: "dimo_valid_cloudevent",
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
		{
			name:           "attestation with dimo.raw.* type",
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.raw.insurance","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{}
			},
			msgLen:        1,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            "dimo.raw.insurance",
				processors.MessageContentKey: "dimo_valid_cloudevent",
			},
		},
		{
			name:           "attestation with dimo.document.* type",
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.document.vehicle.registration","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{}
			},
			msgLen:        1,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            "dimo.document.vehicle.registration",
				processors.MessageContentKey: "dimo_valid_cloudevent",
			},
		},
		{
			name:           "attestation payload source differs from JWT source (delegation)",
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0xABCDEF1234567890ABCDEF1234567890ABCDEF12").String(), // JWT holder differs from payload source
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{}
			},
			msgLen:        1,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            cloudevent.TypeAttestation,
				cloudEventProducerKey:        "0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
				cloudEventSubjectKey:         "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005",
				processors.MessageContentKey: "dimo_valid_cloudevent",
				httpinputserver.DIMOCloudEventSource: common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			},
		},
		{
			name:           "attestation empty payload source falls back to JWT",
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{}
			},
			msgLen:        1,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            cloudevent.TypeAttestation,
				cloudEventProducerKey:        "0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
				cloudEventSubjectKey:         "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005",
				processors.MessageContentKey: "dimo_valid_cloudevent",
				httpinputserver.DIMOCloudEventSource: common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			},
		},
		{
			name:           "attestation invalid payload source",
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"not-an-address","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{}
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:           "connection header with oversized Tags exceeds size cap",
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
					Tags:     []string{strings.Repeat("x", MaxHeaderBytes+1)},
				}
				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: json.RawMessage(`{"key": "value"}`),
					err:  nil,
				}
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:           "connection header with many small Tags totaling above cap",
			inputData:      []byte(`{"test": "data"}`),
			sourceID:       common.HexToAddress("0x").String(),
			messageContent: httpinputserver.ConnectionContent,
			setupMock: func() *mockCloudEventModule {
				tags := make([]string, 1000)
				for i := range tags {
					tags[i] = strings.Repeat("a", 16)
				}
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     timestamp,
					Tags:     tags,
				}
				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: json.RawMessage(`{"key": "value"}`),
					err:  nil,
				}
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name: "attestation with oversized Extras exceeds size cap",
			inputData: []byte(fmt.Sprintf(
				`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","junk":"%s","data":{"x":1}}`,
				attestationTimestamp.Format(time.RFC3339),
				strings.Repeat("z", MaxHeaderBytes+1),
			)),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{}
			},
			msgLen:        1,
			expectedError: true,
			expectedMeta:  nil,
		},
		{
			name:           "connection header with Tags just under size cap succeeds",
			inputData:      []byte(`{"test": "data"}`),
			sourceID:       common.HexToAddress("0x").String(),
			messageContent: httpinputserver.ConnectionContent,
			setupMock: func() *mockCloudEventModule {
				// Aim for ~7 KiB of tag content; well under the 8 KiB cap.
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     timestamp,
					Tags:     []string{strings.Repeat("y", 7000)},
				}
				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: json.RawMessage(`{"key": "value"}`),
					err:  nil,
				}
			},
			msgLen:        1,
			expectedError: false,
			expectedMeta: map[string]any{
				cloudEventTypeKey:            cloudevent.TypeStatus,
				processors.MessageContentKey: "dimo_valid_cloudevent",
			},
		},
		{
			name:           "attestation with invalid type",
			inputData:      []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.signals","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			sourceID:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String(),
			messageContent: httpinputserver.AttestationContent,
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{}
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
			}
		})
	}
}

func TestValidateAndSetContentType(t *testing.T) {
	tests := []struct {
		name                    string
		inputContentType        string
		isBase64                bool
		expectedContentType     string
		expectError             bool
	}{
		{
			name:                "data event empty defaults to application/json",
			inputContentType:    "",
			isBase64:            false,
			expectedContentType: "application/json",
		},
		{
			name:                "data event with application/json passes",
			inputContentType:    "application/json",
			isBase64:            false,
			expectedContentType: "application/json",
		},
		{
			name:             "data event with image/png is rejected",
			inputContentType: "image/png",
			isBase64:         false,
			expectError:      true,
		},
		{
			name:             "data event with arbitrary type is rejected",
			inputContentType: "text/plain",
			isBase64:         false,
			expectError:      true,
		},
		{
			name:             "data_base64 event with empty content type is rejected",
			inputContentType: "",
			isBase64:         true,
			expectError:      true,
		},
		{
			name:                "data_base64 event with image/png passes",
			inputContentType:    "image/png",
			isBase64:            true,
			expectedContentType: "image/png",
		},
		{
			name:                "data_base64 event with image/jpeg passes",
			inputContentType:    "image/jpeg",
			isBase64:            true,
			expectedContentType: "image/jpeg",
		},
		{
			name:                "data_base64 event with application/pdf passes",
			inputContentType:    "application/pdf",
			isBase64:            true,
			expectedContentType: "application/pdf",
		},
		{
			name:                "data_base64 event with application/json passes",
			inputContentType:    "application/json",
			isBase64:            true,
			expectedContentType: "application/json",
		},
		{
			name:             "data_base64 event with non-whitelisted type is rejected",
			inputContentType: "text/plain",
			isBase64:         true,
			expectError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := &cloudevent.CloudEventHeader{DataContentType: tt.inputContentType}
			err := validateAndSetContentType(hdr, tt.isBase64)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedContentType, hdr.DataContentType)
		})
	}
}

func TestParseAndValidateAttestationContentType(t *testing.T) {
	source := common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String()
	timestamp := time.Now().UTC().Format(time.RFC3339)
	baseFields := func(extra string) []byte {
		return []byte(fmt.Sprintf(`{"id":"id1","source":"%s","producer":"%s","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xdeadbeef"%s}`, source, source, timestamp, extra))
	}

	tests := []struct {
		name        string
		input       []byte
		expectError bool
	}{
		{
			name:  "data event with no datacontenttype defaults to json",
			input: baseFields(`,"data":{"k":"v"}`),
		},
		{
			name:        "data event with image/png is rejected",
			input:       baseFields(`,"data":{"k":"v"},"datacontenttype":"image/png"`),
			expectError: true,
		},
		{
			name:  "data_base64 event with image/png is accepted",
			input: baseFields(`,"data_base64":"aGVsbG8=","datacontenttype":"image/png"`),
		},
		{
			name:        "data_base64 event without datacontenttype is rejected",
			input:       baseFields(`,"data_base64":"aGVsbG8="`),
			expectError: true,
		},
		{
			name:        "data_base64 event with non-whitelisted type is rejected",
			input:       baseFields(`,"data_base64":"aGVsbG8=","datacontenttype":"text/plain"`),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseAndValidateAttestation(tt.input, source)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsValidAttestationType(t *testing.T) {
	t.Parallel()

	valid := []string{
		cloudevent.TypeAttestation,
		cloudevent.TypeAttestationTombstone,
		"dimo.raw.insurance",
		"dimo.document.vehicle.registration",
	}
	for _, ty := range valid {
		assert.True(t, isValidAttestationType(ty), "expected %q to be valid", ty)
	}

	invalid := []string{
		"",
		"dimo.raw.",
		"dimo.document.",
		"dimo.signals",
		"dimo.status",
		"random",
	}
	for _, ty := range invalid {
		assert.False(t, isValidAttestationType(ty), "expected %q to be invalid", ty)
	}
}

func TestParseTombstoneData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		data            string
		expectVoidsID   string
		expectErr       bool
		expectErrSubstr string
	}{
		{
			name:          "valid tombstone",
			data:          `{"voidsId":"target-id-1","reason":"uploaded by mistake"}`,
			expectVoidsID: "target-id-1",
		},
		{
			name:          "valid tombstone without reason",
			data:          `{"voidsId":"target-id-1"}`,
			expectVoidsID: "target-id-1",
		},
		{
			name:            "empty data",
			data:            ``,
			expectErr:       true,
			expectErrSubstr: "data payload is required",
		},
		{
			name:            "data is not an object",
			data:            `"just a string"`,
			expectErr:       true,
			expectErrSubstr: "not a tombstone object",
		},
		{
			name:            "missing voidsId",
			data:            `{"reason":"oops"}`,
			expectErr:       true,
			expectErrSubstr: "voidsId is required",
		},
		{
			name:            "empty voidsId",
			data:            `{"voidsId":"","reason":"oops"}`,
			expectErr:       true,
			expectErrSubstr: "voidsId is required",
		},
		{
			name:            "voidsId with disallowed character",
			data:            `{"voidsId":"bad id$"}`,
			expectErr:       true,
			expectErrSubstr: "invalid voidsId",
		},
		{
			name:            "reason exceeds cap",
			data:            fmt.Sprintf(`{"voidsId":"target-id-1","reason":"%s"}`, strings.Repeat("a", MaxTombstoneReasonBytes+1)),
			expectErr:       true,
			expectErrSubstr: "reason length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voidsID, err := parseTombstoneData(json.RawMessage(tt.data))
			if tt.expectErr {
				require.Error(t, err)
				if tt.expectErrSubstr != "" {
					assert.Contains(t, err.Error(), tt.expectErrSubstr)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectVoidsID, voidsID)
		})
	}
}

// signTombstoneEvent builds a signed dimo.tombstone CloudEvent JSON using
// the provided ECDSA key. The returned bytes match the wire format that DIS
// would receive over the attestation endpoint, with a real EOA signature
// over the data field.
func signTombstoneEvent(t *testing.T, key string, source common.Address, subject, id, voidsID, reason string, ts time.Time) []byte {
	t.Helper()
	privKey, err := crypto.HexToECDSA(key)
	require.NoError(t, err)

	dataObj := map[string]string{"voidsId": voidsID}
	if reason != "" {
		dataObj["reason"] = reason
	}
	dataBytes, err := json.Marshal(dataObj)
	require.NoError(t, err)

	hash := accounts.TextHash(dataBytes)
	sig, err := crypto.Sign(hash, privKey)
	require.NoError(t, err)
	// crypto.Sign returns v as 0/1; convert to Ethereum's 27/28.
	sig[64] += 27

	envelope := map[string]any{
		"id":          id,
		"source":      source.Hex(),
		"producer":    source.Hex(),
		"specversion": "1.0",
		"subject":     subject,
		"time":        ts.Format(time.RFC3339),
		"type":        cloudevent.TypeAttestationTombstone,
		"signature":   "0x" + common.Bytes2Hex(sig),
		"data":        json.RawMessage(dataBytes),
	}
	out, err := json.Marshal(envelope)
	require.NoError(t, err)
	return out
}

func TestProcessAttestationMsg_Tombstone(t *testing.T) {
	t.Parallel()

	// Deterministic test key so the test is reproducible.
	const privHex = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
	privKey, err := crypto.HexToECDSA(privHex)
	require.NoError(t, err)
	source := crypto.PubkeyToAddress(privKey.PublicKey)

	subject := "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005"
	now := time.Now().UTC().Truncate(time.Minute)

	t.Run("valid tombstone is accepted and sets dimo_voids_id metadata", func(t *testing.T) {
		input := signTombstoneEvent(t, privHex, source, subject, "tombstone-id-1", "target-attestation-id-1", "uploaded in error", now)

		msg := service.NewMessage(input)
		msg.MetaSet(httpinputserver.DIMOCloudEventSource, source.Hex())
		msg.MetaSet(processors.MessageContentKey, httpinputserver.AttestationContent)

		proc := &cloudeventProcessor{}
		out := proc.processAttestationMsg(context.Background(), msg, input, source.Hex())
		require.Len(t, out, 1)
		require.Nil(t, out[0].GetError(), "unexpected error: %v", out[0].GetError())

		voidsID, ok := out[0].MetaGetMut(rawparquet.MetaVoidsID)
		require.True(t, ok, "expected dimo_voids_id metadata to be set")
		assert.Equal(t, "target-attestation-id-1", voidsID)

		typeMeta, _ := out[0].MetaGetMut(cloudEventTypeKey)
		assert.Equal(t, cloudevent.TypeAttestationTombstone, typeMeta)

		contentMeta, _ := out[0].MetaGetMut(processors.MessageContentKey)
		assert.Equal(t, "dimo_valid_cloudevent", contentMeta)
	})

	t.Run("tombstone with empty voidsId in data is rejected", func(t *testing.T) {
		// Manually craft a tombstone where voidsId is empty but the signature
		// still recovers (we sign over the actual empty-voidsId data).
		dataObj := map[string]string{"voidsId": "", "reason": "x"}
		dataBytes, err := json.Marshal(dataObj)
		require.NoError(t, err)
		hash := accounts.TextHash(dataBytes)
		sig, err := crypto.Sign(hash, privKey)
		require.NoError(t, err)
		sig[64] += 27
		envelope := map[string]any{
			"id":          "tombstone-id-2",
			"source":      source.Hex(),
			"producer":    source.Hex(),
			"specversion": "1.0",
			"subject":     subject,
			"time":        now.Format(time.RFC3339),
			"type":        cloudevent.TypeAttestationTombstone,
			"signature":   "0x" + common.Bytes2Hex(sig),
			"data":        json.RawMessage(dataBytes),
		}
		input, err := json.Marshal(envelope)
		require.NoError(t, err)

		msg := service.NewMessage(input)
		msg.MetaSet(httpinputserver.DIMOCloudEventSource, source.Hex())
		msg.MetaSet(processors.MessageContentKey, httpinputserver.AttestationContent)

		proc := &cloudeventProcessor{}
		out := proc.processAttestationMsg(context.Background(), msg, input, source.Hex())
		require.Len(t, out, 1)
		require.NotNil(t, out[0].GetError(), "expected error for empty voidsId")
	})

	// Bad-signature behavior is identical for all attestation flavors and is
	// covered by the existing TestProcessBatch cases for `dimo.attestation`;
	// no need to duplicate it here.
}
