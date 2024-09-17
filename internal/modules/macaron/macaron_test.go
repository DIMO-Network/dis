package macaron_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/DIMO-Network/DIS/internal/modules/macaron"
	"github.com/DIMO-Network/DIS/internal/service/deviceapi"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/stretchr/testify/require"
)

const (
	notFoundSubject = "not_found"
	errorSubject    = "error"
)

func TestVSSProcessorProcess(t *testing.T) {
	tests := []struct {
		name            string
		msgBytes        []byte
		expectedSignals []vss.Signal
		expectedErr     bool
	}{
		{
			name:     "v1 payload",
			msgBytes: []byte(`{"specversion":"1.0", "time": "2024-12-23T12:34:00Z", "source": "source1", "subject": "1" "data"{"speed": 1.0}}`),
			expectedSignals: []vss.Signal{
				{
					TokenID:     1,
					Timestamp:   time.UnixMilli(1734957240000).UTC(),
					Name:        vss.FieldSpeed,
					Source:      "source1",
					ValueNumber: 1.0,
				},
			},
		},
		{
			name:     "v2 payload",
			msgBytes: []byte(`{"dataschema":"v2.0", "specversion":"1.0", "vehicleTokenId": 1, "source": "source1", "data": {"vehicle": {"signals": [{"name": "speed", "timestamp": 1734957240000, "value": 1.0}]}}}`),
			expectedSignals: []vss.Signal{
				{
					TokenID:     1,
					Timestamp:   time.UnixMilli(1734957240000).UTC(),
					Name:        vss.FieldSpeed,
					Source:      "source1",
					ValueNumber: 1.0,
				},
			},
		},
		{
			name:     "v2 dataSchema",
			msgBytes: []byte(`{"specversion":"1.0","dataschema":"dimo.zone.status/v2.0", "vehicleTokenId": 1, "source": "source1", "data": {"vehicle": {"signals": [{"name": "speed", "timestamp": 1734957240000, "value": 1.0}]}}}`),
			expectedSignals: []vss.Signal{
				{
					TokenID:     1,
					Timestamp:   time.UnixMilli(1734957240000).UTC(),
					Name:        vss.FieldSpeed,
					Source:      "source1",
					ValueNumber: 1.0,
				},
			},
		},
		{
			name:        "converted v2 payload",
			msgBytes:    []byte(`{"specversion":"1.0","dataschema":"dimo.zone.status/v1.1", "source": "source1", "time": "2024-12-23T12:34:00Z", "subject": "1", "data"{"speed": 1.0}}`),
			expectedErr: false,
		},

		{
			name:        "unknown version",
			msgBytes:    []byte(`{"specversion":"1.0","dataschema":"dimo.zone.status/v3.0", "vehicleTokenId": 1, "source": "source1", "data": {"vehicle": {"signals": [{"name": "speed", "timestamp": 1734957240000, "value": 1.0}]}}}`),
			expectedErr: true,
		},
		{
			name:        "no tokenID for subject",
			msgBytes:    []byte(`{"specversion":"1.0", "source": "source1", "time": "2024-12-23T12:34:00Z", "subject": "not_found", "data"{"speed": 1.0}}`),
			expectedErr: false,
		},
		{
			name:        "error getting tokenID",
			msgBytes:    []byte(`{"specversion":"1.0", "source": "source1", "time": "2024-12-23T12:34:00Z", "subject": "error", "data"{"speed": 1.0}}`),
			expectedErr: true,
		},
	}

	macaronModule := macaron.MacaronModule{
		TokenGetter: &testGetter{},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			signals, err := macaronModule.SignalConvert(context.Background(), test.msgBytes)
			if test.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Len(t, signals, len(test.expectedSignals))
			require.Equal(t, test.expectedSignals, signals)
		})
	}
}

type testGetter struct{}

func (*testGetter) TokenIDFromSubject(_ context.Context, subject string) (uint32, error) {
	if subject == notFoundSubject {
		return 0, fmt.Errorf("%w: no tokenID set", deviceapi.NotFoundError{DeviceID: subject})
	}
	if subject == errorSubject {
		return 0, errors.New("test error")
	}
	id, err := strconv.Atoi(subject)
	return uint32(id), err
}
