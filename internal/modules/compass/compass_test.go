package compass

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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
			input:            []byte(`{"time":"2022-01-01T00:00:00Z","data":{"id":"S76960rsT8SYlrvlTfMWoQ==","vehicle_id":"1C4SJSBP8RS133747","timestamp":{"seconds":1737988799},"transport_type":0,"vehicle_type":0,"position":{"latlng":{"lat":34.821937,"lng":-82.291492}},"ingested_at":{"seconds":1737988847,"nanos":326690000}}","vehicleTokenId":1, "deviceTokenId":2}`),
			expectError:      false,
			length:           2,
			expectedSubject:  "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_1",
			expectedProducer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
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
