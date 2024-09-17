package signalconvert

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/DIMO-Network/DIS/internal/service/deviceapi"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

const (
	notFoundSubject = "not_found"
	errorSubject    = "error"
)

func TestVSSProcessorProcess(t *testing.T) {
	tests := []struct {
		name          string
		msg           *service.Message
		expectedBatch func() service.MessageBatch
		expectedErr   bool
	}{
		{
			name: "v1 payload",
			msg:  service.NewMessage([]byte(`{"specversion":"1.0", "time": "2024-12-23T12:34:00Z", "source": "source1", "subject": "1" "data"{"speed": 1.0}}`)),
			expectedBatch: func() service.MessageBatch {
				msg := service.NewMessage(nil)
				sig := vss.Signal{
					TokenID:     1,
					Timestamp:   time.UnixMilli(1734957240000).UTC(),
					Name:        vss.FieldSpeed,
					Source:      "source1",
					ValueNumber: 1.0,
				}
				msg.SetStructured(vss.SignalToSlice(sig))
				return []*service.Message{msg}
			},
		},
		{
			name: "v2 payload",
			msg:  service.NewMessage([]byte(`{"dataschema":"v2.0", "specversion":"1.0", "vehicleTokenId": 1, "source": "source1", "data": {"vehicle": {"signals": [{"name": "speed", "timestamp": 1734957240000, "value": 1.0}]}}}`)),
			expectedBatch: func() service.MessageBatch {
				msg := service.NewMessage(nil)
				sig := vss.Signal{
					TokenID:     1,
					Timestamp:   time.UnixMilli(1734957240000).UTC(),
					Name:        vss.FieldSpeed,
					Source:      "source1",
					ValueNumber: 1.0,
				}
				msg.SetStructured(vss.SignalToSlice(sig))
				return []*service.Message{msg}
			},
		},
		{
			name: "v2 dataSchema",
			msg:  service.NewMessage([]byte(`{"specversion":"1.0","dataschema":"dimo.zone.status/v2.0", "vehicleTokenId": 1, "source": "source1", "data": {"vehicle": {"signals": [{"name": "speed", "timestamp": 1734957240000, "value": 1.0}]}}}`)),
			expectedBatch: func() service.MessageBatch {
				msg := service.NewMessage(nil)
				sig := vss.Signal{
					TokenID:     1,
					Timestamp:   time.UnixMilli(1734957240000).UTC(),
					Name:        vss.FieldSpeed,
					Source:      "source1",
					ValueNumber: 1.0,
				}
				msg.SetStructured(vss.SignalToSlice(sig))
				return []*service.Message{msg}
			},
		},
		{
			name:          "converted v2 payload",
			msg:           service.NewMessage([]byte(`{"specversion":"1.0","dataschema":"dimo.zone.status/v1.1", "source": "source1", "time": "2024-12-23T12:34:00Z", "subject": "1", "data"{"speed": 1.0}}`)),
			expectedBatch: func() service.MessageBatch { return nil },
			expectedErr:   false,
		},

		{
			name:          "unknown version",
			msg:           service.NewMessage([]byte(`{"specversion":"1.0","dataschema":"dimo.zone.status/v3.0", "vehicleTokenId": 1, "source": "source1", "data": {"vehicle": {"signals": [{"name": "speed", "timestamp": 1734957240000, "value": 1.0}]}}}`)),
			expectedBatch: func() service.MessageBatch { return nil },
			expectedErr:   true,
		},
		{
			name:          "no tokenID for subject",
			msg:           service.NewMessage([]byte(`{"specversion":"1.0", "source": "source1", "time": "2024-12-23T12:34:00Z", "subject": "not_found", "data"{"speed": 1.0}}`)),
			expectedBatch: func() service.MessageBatch { return nil },
			expectedErr:   false,
		},
		{
			name:          "error getting tokenID",
			msg:           service.NewMessage([]byte(`{"specversion":"1.0", "source": "source1", "time": "2024-12-23T12:34:00Z", "subject": "error", "data"{"speed": 1.0}}`)),
			expectedBatch: func() service.MessageBatch { return nil },
			expectedErr:   true,
		},
	}

	vssProc := &vssProcessor{
		tokenGetter: &testGetter{},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batches, err := vssProc.ProcessBatch(context.Background(), []*service.Message{test.msg})
			require.NoError(t, err)
			if test.expectedErr {
				for _, batch := range batches {
					for _, msg := range batch {
						err = msg.GetError()
						if err != nil {
							return
						}
					}
				}
				require.Fail(t, "expected an error from the processor")
			}

			var batch service.MessageBatch
			if len(batches) > 1 {
				require.Fail(t, "expected a single or no batches")
			} else if len(batches) == 1 {
				batch = batches[0]
			}

			var expectedBatch service.MessageBatch
			if test.expectedBatch != nil {
				expectedBatch = test.expectedBatch()
			}
			require.Len(t, expectedBatch, len(batch))
			for i := range batch {
				expectedBytes, err := expectedBatch[i].AsBytes()
				require.NoError(t, err)
				actualBytes, err := batch[i].AsBytes()
				require.NoError(t, err)
				require.Equal(t, expectedBytes, actualBytes)
			}
		})
	}
}

type testGetter struct{}

func (t *testGetter) TokenIDFromSubject(ctx context.Context, subject string) (uint32, error) {
	if subject == notFoundSubject {
		return 0, fmt.Errorf("%w: no tokenID set", deviceapi.NotFoundError{DeviceID: subject})
	}
	if subject == errorSubject {
		return 0, errors.New("test error")
	}
	id, err := strconv.Atoi(subject)
	return uint32(id), err
}
