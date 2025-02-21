package compass

import (
	"context"
	"encoding/json"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCloudEventConvert(t *testing.T) {
	module := Module{}
	err := module.SetConfig(`{"chain_id":1,"synth_contract_addr":"0x78513c8CB4D6B6079f813850376bc9c7fc8aE67f","vehicle_contract_addr":"0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"}`)
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
			name:             "Status payload with VIN",
			input:            []byte(`{"time":"2022-01-01T00:00:00Z","data":{"id":"S76960rsT8SYlrvlTfMWoQ==","vehicle_id":"1C4SJSBP8RS133747","timestamp":{"seconds":1737988799},"transport_type":0,"vehicle_type":0,"position":{"latlng":{"lat":34.821937,"lng":-82.291492}},"ingested_at":{"seconds":1737988847,"nanos":326690000}},"vehicleTokenId":1,"deviceTokenId":2}`),
			expectError:      false,
			length:           2,
			expectedSubject:  "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
			expectedProducer: "did:nft:1:0x78513c8CB4D6B6079f813850376bc9c7fc8aE67f_2",
		},
		{
			name:             "Status payload with no VIN",
			input:            []byte(`{"time":"2022-01-01T00:00:00Z","data":{"id":"S76960rsT8SYlrvlTfMWoQ==","timestamp":{"seconds":1737988799},"transport_type":0,"vehicle_type":0,"position":{"latlng":{"lat":34.821937,"lng":-82.291492}},"ingested_at":{"seconds":1737988847,"nanos":326690000}},"vehicleTokenId":1,"deviceTokenId":2}`),
			expectError:      false,
			length:           1,
			expectedSubject:  "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
			expectedProducer: "did:nft:1:0x78513c8CB4D6B6079f813850376bc9c7fc8aE67f_2",
		},
		{
			name:             "Status payload with no vehicleTokenId",
			input:            []byte(`{"time":"2022-01-01T00:00:00Z","data":{"id":"S76960rsT8SYlrvlTfMWoQ==","vehicle_id":"1C4SJSBP8RS133747","timestamp":{"seconds":1737988799},"transport_type":0,"vehicle_type":0,"position":{"latlng":{"lat":34.821937,"lng":-82.291492}},"ingested_at":{"seconds":1737988847,"nanos":326690000}},"deviceTokenId":2}`),
			expectError:      false,
			length:           2,
			expectedSubject:  "",
			expectedProducer: "did:nft:1:0x78513c8CB4D6B6079f813850376bc9c7fc8aE67f_2",
		},
		{
			name:        "Status payload with no deviceTokenId",
			input:       []byte(`{"time":"2022-01-01T00:00:00Z","data":{"id":"S76960rsT8SYlrvlTfMWoQ==","vehicle_id":"1C4SJSBP8RS133747","timestamp":{"seconds":1737988799},"transport_type":0,"vehicle_type":0,"position":{"latlng":{"lat":34.821937,"lng":-82.291492}},"ingested_at":{"seconds":1737988847,"nanos":326690000}}, "vehicleTokenId":1}`),
			expectError: true,
			length:      1,
		},
		{
			name:        "Invalid input",
			input:       []byte(`invalid`),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdrs, _, err := module.CloudEventConvert(context.Background(), tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, hdrs, tt.length)

				cloudEvent := hdrs[0]
				assert.Equal(t, tt.expectedSubject, cloudEvent.Subject)
				assert.Equal(t, tt.expectedProducer, cloudEvent.Producer)
			}
		})
	}
}

const compassConnection = "0x55BF1c27d468314Ea119CF74979E2b59F962295c"

func TestSignalConvert(t *testing.T) {
	ts := time.Unix(1727360340, 0).UTC()

	// Signal payload data
	signalData := `{
    "labels":{
         "engine.oil.pressure.unit":"bar",
         "tirePressure.front.left.unit":"psi",
         "engine.status":"off",
         "geolocation.altitude.unit":"m",
         "engine.oil.pressure.value":"0.04",
         "engine.oil.lifeLeft.percentage":"90",
         "datetime":"2025-02-06T15:18:22.243Z",
         "odometer.value":"20446",
         "vin":"1C4SJSBP8RS133747",
         "geolocation.latitude":"34.878016",
         "moving":"false",
         "engine.speed.unit":"rpm",
         "fuel.residualAutonomy.value":"733",
         "transmissionGear.state":"p",
         "seatbelt.passenger.front":"false",
         "geolocation.longitude":"-82.223566",
         "doors.open.passenger.front":"false",
         "adas.abs":"false",
         "engine.battery.voltage.value":"13",
         "doors.open.driver":"false",
         "fuel.level.percentage":"99",
         "engine.speed.value":"0",
         "lights.fog.front":"false",
         "tirePressure.front.right.unit":"psi",
         "speed.unit":"km/h",
         "engine.ignition":"false",
         "engine.contact":"false",
         "_id":"6786afd0ff54c000078aa67a",
         "tirePressure.rear.right.unit":"psi",
         "status":"halted",
         "engine.coolant.temperature.value":"92",
         "engine.battery.charging":"false",
         "speed.value":"40.2336",
         "fuel.averageConsumption.value":"6.4",
         "odometer.unit":"km",
         "fuel.level.unit":"L",
         "engine.battery.voltage.unit":"V",
         "engine.oil.temperature.value":"89",
         "tirePressure.front.right.value":"41",
         "engine.oil.temperature.unit":"°C",
         "fuel.averageConsumption.unit":"L/100 km",
         "seatbelt.driver":"false",
         "tirePressure.front.left.value":"41",
         "fuel.residualAutonomy.unit":"km",
         "doors.open.passenger.rear.left":"false",
         "tirePressure.rear.left.unit":"psi",
         "heading":"16",
         "lights.fog.rear":"false",
         "doors.open.passenger.rear.right":"false",
         "tirePressure.rear.left.value":"42",
         "fuel.level.value":"113.85",
         "engine.coolant.temperature.unit":"°C",
         "datetimeSending":"2025-02-06T15:18:22.243Z",
         "crash.autoEcall":"false",
         "tirePressure.rear.right.value":"41",
         "geolocation.altitude.value":"277.100006"
      }}`

	const source = "0x55BF1c27d468314Ea119CF74979E2b59F962295c"
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
					DataVersion: "1.0",
					Type:        cloudevent.TypeStatus,
					Source:      source,
					Subject:     "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_37",
					Time:        ts,
				},
				Data: json.RawMessage(signalData),
			},
			expectedSignals: []vss.Signal{
				{TokenID: 37, Timestamp: ts, Name: vss.FieldChassisAxleRow1WheelLeftTirePressure, ValueNumber: 282.68516, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldChassisAxleRow1WheelRightTirePressure, ValueNumber: 282.68516, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldChassisAxleRow2WheelLeftTirePressure, ValueNumber: 289.57992, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldChassisAxleRow2WheelRightTirePressure, ValueNumber: 282.68516, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldCurrentLocationLatitude, ValueNumber: 34.878016, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldCurrentLocationLongitude, ValueNumber: -82.223566, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldCurrentLocationAltitude, ValueNumber: 277.100006, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldCurrentLocationHeading, ValueNumber: 16, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldIsIgnitionOn, ValueNumber: 0, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldLowVoltageBatteryCurrentVoltage, ValueNumber: 13, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldPowertrainCombustionEngineECT, ValueNumber: 92, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldPowertrainCombustionEngineEOP, ValueNumber: 4, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldPowertrainCombustionEngineEOT, ValueNumber: 89, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldPowertrainCombustionEngineSpeed, ValueNumber: 0, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldPowertrainFuelSystemAbsoluteLevel, ValueNumber: 113.85, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldPowertrainFuelSystemRelativeLevel, ValueNumber: 99, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldPowertrainTransmissionTravelledDistance, ValueNumber: 20446, Source: compassConnection},
				{TokenID: 37, Timestamp: ts, Name: vss.FieldSpeed, ValueNumber: 40.2336, Source: compassConnection},
			},
			expectedError: nil,
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
				assert.ElementsMatch(t, signals, tt.expectedSignals)
			}
		})
	}
}
