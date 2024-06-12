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

func TestNameIndexerProcessor(t *testing.T) {
	defaultConfig := `
timestamp: '${!json("time")}'
primary_filler: 'MM'
secondary_filler: '00'
data_type: 'FP/v0.0.1'
address: '${!json("subject")}'
`

	tests := []struct {
		name         string
		jsonString   string
		config       string
		expectedMeta string
		expectErr    bool
	}{
		{
			name: "Valid name message with default fillers",
			jsonString: `{
				"time": "2024-06-11T15:30:00Z",
				"subject": "0xc57d6d57fca59d0517038c968a1b831b071fa679"
			}`,
			config: defaultConfig,
			expectedMeta: mustEncode(&nameindexer.Index{
				Timestamp:       time.Date(2024, 6, 11, 15, 30, 0, 0, time.UTC),
				PrimaryFiller:   "MM",
				SecondaryFiller: "00",
				DataType:        "FP/v0.0.1",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
			}),
			expectErr: false,
		},
		{
			name:         "Invalid JSON message",
			jsonString:   `invalid json`,
			config:       defaultConfig,
			expectedMeta: "",
			expectErr:    true,
		},
		{
			name: "Invalid subject (address)",
			jsonString: `{
				"time": "2024-06-11T15:30:00Z",
				"subject": "invalid_address"
			}`,
			config:       defaultConfig,
			expectedMeta: "",
			expectErr:    true,
		},
		{
			name: "Missing subject",
			jsonString: `{
				"time": "2024-06-11T15:30:00Z"
			}`,
			config:       defaultConfig,
			expectedMeta: "",
			expectErr:    true,
		},
		{
			name: "Missing time",
			jsonString: `{
				"subject": "0xc57d6d57fca59d0517038c968a1b831b071fa679"
			}`,
			config:       defaultConfig,
			expectedMeta: "",
			expectErr:    true,
		},
		{
			name: "Missing time",
			jsonString: `{
				"subject": "0xc57d6d57fca59d0517038c968a1b831b071fa679"
			}`,
			config:       defaultConfig,
			expectedMeta: "",
			expectErr:    true,
		},
		{
			name: "Custom fillers and data type",
			jsonString: `{
				"time": "2024-06-11T15:30:00Z",
				"subject": "0xc57d6d57fca59d0517038c968a1b831b071fa679"
			}`,
			config: `
timestamp: '${!now()}'
primary_filler: 'XX'
secondary_filler: 'YY'
data_type: 'CustomType'
address: '${!json("subject")}'
`,
			expectedMeta: mustEncode(&nameindexer.Index{
				Timestamp:       time.Now(),
				PrimaryFiller:   "XX",
				SecondaryFiller: "YY",
				DataType:        "CustomType",
				Address:         common.HexToAddress("0xc57d6d57fca59d0517038c968a1b831b071fa679"),
			}),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parsedConfig, err := configSpec.ParseYAML(tt.config, nil)
			require.NoError(t, err)
			processor, err := ctor(parsedConfig, nil)
			require.NoError(t, err)

			msg := service.NewMessage([]byte(tt.jsonString))
			batch, err := processor.Process(context.Background(), msg)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, batch)
				require.Len(t, batch, 1)
				encodedIndex, ok := batch[0].MetaGet("index")
				require.True(t, ok)
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
