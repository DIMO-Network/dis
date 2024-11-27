package autopi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudEventConvert(t *testing.T) {
	module := Module{}
	err := module.SetConfig(`{"chain_id":2,"aftermarket_contract_addr":"0x325b45949C833986bC98e98a49F3CA5C5c4643B5","vehicle_contract_addr":"0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8"}`)
	require.NoError(t, err)
	tests := []struct {
		name             string
		input            []byte
		expectError      bool
		length           int
		expectedSubject  string
		expectedProducer string
	}{
		{
			name:             "Status payload",
			input:            []byte(`{"data":{"device":{"serial":"60d4af69-86e8-b790-02d3-c0a9dc4d6c8a","softwareVersion":"v1.0.0"},"timestamp":1732224181876,"vehicle":{"make":"MINI","model":"Countryman","signals":[{"name":"batteryVoltage","timestamp":1732224181876,"value":12.95}],"year":2018}},"signature":"0x67bdfbfce03ef7c6577a4a64de037db97d882ef158ee6d1b3adc96e0e58599b2508bb74f8780e102e0c50b7b30385ed6160aa8218c9793cb00fc8f8b355a966c1b","time":"2024-11-21T21:23:01.876617869Z","type":"com.dimo.device.status.v2","vehicleTokenId":1, "deviceTokenId": 2222}`),
			expectError:      false,
			length:           2,
			expectedSubject:  "did:nft:2:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8_1",
			expectedProducer: "did:nft:2:0x325b45949C833986bC98e98a49F3CA5C5c4643B5_2222",
		},
		{
			name:             "Status payload with no vehicleTokenId",
			input:            []byte(`{"data":{"device":{"softwareVersion":"v1.0.0"},"timestamp":1732224181876,"vehicle":{"make":"MINI","model":"Countryman","year":2018}},"time":"2024-11-21T21:23:01.876617869Z","type":"com.dimo.device.status.v2", "deviceTokenId": 2222}`),
			expectError:      false,
			length:           2,
			expectedSubject:  "",
			expectedProducer: "did:nft:2:0x325b45949C833986bC98e98a49F3CA5C5c4643B5_2222",
		},
		{
			name:        "Status payload, device token id is missing",
			input:       []byte(`{"type":"com.dimo.device.status.v2"}`),
			expectError: true,
		},
		{
			name:             "Fingerprint payload",
			input:            []byte(`{"subject":"0x1234567890abcdef1234567890abcdef12345678","time":"2023-10-31T12:34:56Z","type":"zone.dimo.aftermarket.device.fingerprint","vehicleTokenId":1, "deviceTokenId": 2222, "data":{"timestamp":1638316800000,"device":{"rpiUptimeSecs":3600,"batteryVoltage":12.6,"softwareVersion":"1.0.0","hwVersion":"v1","imei":"123456789012345","serial":"unit123"},"vin":"1HGCM82633A123456","protocol":"ISO9141","odometer":12345.67}}`),
			expectError:      false,
			length:           2,
			expectedSubject:  "did:nft:2:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8_1",
			expectedProducer: "did:nft:2:0x325b45949C833986bC98e98a49F3CA5C5c4643B5_2222",
		},
		{
			name:        "Unknown payload type",
			input:       []byte(`{"subject":"0x1234567890abcdef1234567890abcdef12345678","time":"2023-10-31T12:34:56Z","type":"some","vehicleTokenId":1, "deviceTokenId": 2222}`),
			expectError: true,
		},
		{
			name:        "Invalid input",
			input:       []byte(`invalid`),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events, err := module.CloudEventConvert(context.Background(), tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, events, tt.length)

				var cloudEvent cloudevent.CloudEvent[json.RawMessage]
				errUnmarshal := json.Unmarshal(events[0], &cloudEvent)
				if errUnmarshal != nil {
					t.Fatalf("Failed to unmarshal cloud event: %v", errUnmarshal)
				}

				assert.Equal(t, tt.expectedSubject, cloudEvent.Subject)
				assert.Equal(t, tt.expectedProducer, cloudEvent.Producer)
			}
		})
	}
}

func TestSignalConvert(t *testing.T) {
	ts := time.Unix(1727360340, 0).UTC()

	// Signal payload data
	signalData := `{
    "vehicle": {		
         "signals": [
			 {
                    "timestamp": 1727360340000,
                    "name": "longTermFuelTrim1",
                    "value": 25
                },
                {
                    "timestamp": 1727360340000,
                    "name": "coolantTemp",
                    "value": 107
                }
		]
    }}`

	fingerPrintData := `{"timestamp":1638316800000,"device":{"rpiUptimeSecs":3600,"batteryVoltage":12.6,"softwareVersion":"1.0.0","hwVersion":"v1","imei":"123456789012345","serial":"unit123"},"vin":"1HGCM82633A123456","protocol":"ISO9141","odometer":12345.67}`

	const source = "dimo/integration/27qftVRWQYpVDcO5DltO5Ojbjxk"
	tests := []struct {
		name            string
		cloudEvent      cloudevent.CloudEvent[json.RawMessage]
		expectedSignals []vss.Signal
		expectedError   error
	}{
		{
			name: "Valid Signal Payload",
			cloudEvent: cloudevent.CloudEvent[json.RawMessage]{
				CloudEventHeader: cloudevent.CloudEventHeader{
					DataVersion: DataVersion,
					Type:        cloudevent.TypeStatus,
					Source:      source,
					Subject:     "did:nft:1:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8_33",
					Time:        ts,
				},
				Data: json.RawMessage(signalData),
			},
			expectedSignals: []vss.Signal{
				{TokenID: 33, Timestamp: ts, Name: vss.FieldOBDLongTermFuelTrim1, ValueNumber: 25, Source: source},
				{TokenID: 33, Timestamp: ts, Name: vss.FieldPowertrainCombustionEngineECT, ValueNumber: 107, Source: source},
			},
			expectedError: nil,
		},
		{
			name: "Device Status Payload",
			cloudEvent: cloudevent.CloudEvent[json.RawMessage]{
				CloudEventHeader: cloudevent.CloudEventHeader{
					DataVersion: DataVersion,
					Type:        cloudevent.TypeStatus,
					Source:      source,
					Subject:     "did:nft:1:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8_33",
					Producer:    "did:nft:1:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8_33",
					Time:        ts,
				},
				Data: json.RawMessage(signalData),
			},
			expectedSignals: nil,
			expectedError:   nil,
		},
		{
			name: "Fingerprint Payload",
			cloudEvent: cloudevent.CloudEvent[json.RawMessage]{
				CloudEventHeader: cloudevent.CloudEventHeader{
					DataVersion: DataVersion,
					Type:        cloudevent.TypeFingerprint,
					Source:      source,
					Subject:     "did:nft:1:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8_33",
					Time:        ts,
				},
				Data: json.RawMessage(fingerPrintData),
			},
			expectedSignals: nil,
			expectedError:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal CloudEvent to JSON
			msgBytes, err := json.Marshal(tt.cloudEvent)
			require.NoError(t, err)

			// Call the SignalConvert function
			module := Module{}
			signals, err := module.SignalConvert(context.Background(), msgBytes)

			if tt.expectedError != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedSignals, signals)
			}
		})
	}
}
