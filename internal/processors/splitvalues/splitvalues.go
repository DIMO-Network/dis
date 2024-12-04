package splitvalues

import (
	"context"
	"errors"
	"fmt"

	"github.com/DIMO-Network/dis/internal/processors/cloudeventconvert"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var configSpec = service.NewConfigSpec().
	Summary("Pulls index values off a cloudevent message and returns seperate messagaes for each field")

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

// ProcessBatch to fulfill the service.Processor sets an error on each message.
func (p processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retMsgs []*service.Message
	for _, msg := range msgs {
		values, ok := msg.MetaGetMut(cloudeventconvert.CloudEventIndexValueKey)
		if !ok {
			return nil, errors.New("no index values found")
		}
		allValSlices, ok := values.([]any)
		if !ok {
			return nil, fmt.Errorf("index values is not a slice of slices instead is %T", values)
		}
		for _, valSlice := range allValSlices {
			vals, ok := valSlice.([]any)
			if !ok {
				return nil, fmt.Errorf("index value is not a slice instead is %T", vals)
			}
			newMsg := msg.Copy()
			newMsg.SetStructured(vals)
			retMsgs = append(retMsgs, newMsg)
		}
	}
	return []service.MessageBatch{retMsgs}, nil
}