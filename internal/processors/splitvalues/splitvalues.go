package splitvalues

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/clickhouse"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cloudEventIndexValueKey = "dimo_cloudevent_index_value"
	cloudeventIndexKey      = "dimo_cloudevent_index"
)

var configSpec = service.NewConfigSpec().
	Summary("Pulls index values off a cloudevent message and returns separate messages for each field")

func init() {
	err := service.RegisterBatchProcessor("dimo_split_values", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return processor{}, nil
}

type processor struct{}

// Close to fulfill the service.Processor interface.
func (processor) Close(context.Context) error { return nil }

// ProcessBatch splits each message into separate messages for each CloudEvent header.
// When flowing through inproc, the metadata value is a Go slice (not a JSON string),
// so we handle both cases.
func (p processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retMsgs []*service.Message
	for _, msg := range msgs {
		values, ok := msg.MetaGetMut(cloudEventIndexValueKey)
		if !ok {
			return nil, errors.New("no index values found")
		}

		hdrs, err := toHeaders(values)
		if err != nil {
			return nil, err
		}

		indexKey, ok := msg.MetaGet(cloudeventIndexKey)
		if !ok {
			return nil, errors.New("no index key found")
		}
		for _, hdr := range hdrs {
			vals := clickhouse.CloudEventToSliceWithKey(hdr, indexKey)
			newMsg := msg.Copy()
			newMsg.SetStructured(vals)
			retMsgs = append(retMsgs, newMsg)
		}
	}
	return []service.MessageBatch{retMsgs}, nil
}

// toHeaders converts the metadata value to a slice of CloudEventHeader pointers.
// Via inproc the value is already a Go slice; via Kafka/other transports it's a JSON string.
func toHeaders(values any) ([]*cloudevent.CloudEventHeader, error) {
	switch v := values.(type) {
	case []*cloudevent.CloudEventHeader:
		return v, nil
	case []cloudevent.CloudEventHeader:
		ptrs := make([]*cloudevent.CloudEventHeader, len(v))
		for i := range v {
			ptrs[i] = &v[i]
		}
		return ptrs, nil
	case string:
		var hdrs []*cloudevent.CloudEventHeader
		if err := json.Unmarshal([]byte(v), &hdrs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal cloud event headers: %w", err)
		}
		return hdrs, nil
	default:
		return nil, fmt.Errorf("index values has unexpected type %T", values)
	}
}
