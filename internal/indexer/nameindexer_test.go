package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/nameindexer"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestFingerprintIndexerProcessor(t *testing.T) {
	config := `
timestamp: '${!json("time")}'
primary_filler: 'MM'
secondary_filler: '00'
data_type: 'FP/v0.0.1'
address: '${!json("subject")}'
`
	parsedConfig, err := configSpec.ParseYAML(config, nil)
	require.NoError(t, err)

	processor, err := ctor(parsedConfig, nil)
	require.NoError(t, err)
	ctx := context.Background()

	tests := []struct {
		name         string
		jsonString   string
		expectedMeta string
		expectErr    bool
	}{
		{
			name: "Valid fingerprint message with default fillers",
			jsonString: `{
				"time": "2024-06-11T15:30:00Z",
				"subject": "0xc57d6d57fca59d0517038c968a1b831b071fa679"
			}`,
			expectedMeta: mustEncode(&nameindexer.Index{
				Timestamp: time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Address:   common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
				DataType:  "FP/v0.0.1",
			}),
			expectErr: false,
		},
		{
			name:         "Invalid JSON message",
			jsonString:   `invalid json`,
			expectedMeta: "",
			expectErr:    true,
		},
		{
			name: "Invalid subject (address)",
			jsonString: `{
				"time": "2024-06-11T15:30:00Z",
				"subject": "invalid_address"
			}`,
			expectedMeta: mustEncode(&nameindexer.Index{
				Timestamp: time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				Address:   common.HexToAddress(""),
				DataType:  "FP/v0.0.1",
			}),
			expectErr: false,
		},
		{
			name: "Missing subject",
			jsonString: `{
				"time": "2024-06-11T15:30:00Z"
			}`,
			expectErr: true,
		},

		{
			name: "Missing Time",
			jsonString: `{
				"subject": "0xc57d6d57fca59d0517038c968a1b831b071fa679"
			}`,
			expectErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := service.NewMessage([]byte(tt.jsonString))
			batch, err := processor.Process(ctx, msg)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, batch)
				require.Len(t, batch, 1)
				encodedIndex, ok := batch[0].MetaGet("index")
				require.Truef(t, ok, "index meta not found in message: %v", batch[0])
				require.Equal(t, tt.expectedMeta, encodedIndex)
			}
		})
	}
}

func mustEncode(index *nameindexer.Index) string {
	encodedIndex, err := nameindexer.EncodeIndex(index)
	if err != nil {
		panic(err)
	}
	return encodedIndex
}
