package ruptela

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloudEventConvert(t *testing.T) {
	module := RuptelaModule{}

	tests := []struct {
		name        string
		input       []byte
		expectError bool
		length      int
	}{
		{
			name:        "Status payload with VIN",
			input:       []byte(`{"ds":"r/v0/s","signature":"test","time":"2022-01-01T00:00:00Z","data":{"signals":{"104":"4148544241334344","105":"3930363235323539","106":"3300000000000000"}},"subject":"test","vehicleTokenId":1}`),
			expectError: false,
			length:      2,
		},
		{
			name:        "Status payload with no VIN",
			input:       []byte(`{"ds":"r/v0/s","signature":"test","time":"2022-01-01T00:00:00Z","data":{"trigger":409,"prt":1,"signals":{"104":"0","105":"0","106":"0"}}}`),
			expectError: false,
			length:      1,
		},
		{
			name:        "Location payload",
			input:       []byte(`{"ds":"r/v0/s","signature":"test","time":"2022-01-01T00:00:00Z","data":{"location":[{"ts":1727712225,"lat":522784033,"lon":-9085750,"alt":1049,"dir":22390,"hdop":50},{"ts":1727712226,"lat":522783650,"lon":-9086150,"alt":1044,"dir":20100,"hdop":50}]}}`),
			expectError: false,
			length:      1,
		},
		{
			name:        "Dev status payload",
			input:       []byte(`{"ds":"r/v0/dev","signature":"test","time":"2022-01-01T00:00:00Z","data":{"sn":"869267077308554","battVolt":"12420","hwVersion":"FTX-04-12231","imei":"869267077308554","fwVersion":"00.06.56.45","sigStrength":"14","accessTech":"0","operator":"23415","locAreaCode":"13888","cellId":"29443"}}`),
			expectError: true,
		},
		{
			name:        "Invalid time format",
			input:       []byte(`{"ds":"r/v0/loc","signature":"test","time":"1727712275"}`),
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
				assert.NoError(t, err)
				assert.Equal(t, tt.length, len(events))
			}
		})
	}
}
