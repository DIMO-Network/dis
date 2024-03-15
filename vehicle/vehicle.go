package vehicle

import (
	"context"
	"fmt"

	"github.com/KevinJoiner/model-garage/pkg/vss"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec()
	ctor := func(_ *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newVSSProcessor(mgr.Logger()), nil
	}
	err := service.RegisterProcessor("vss_vehicle", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

type vssProcessor struct {
	logger *service.Logger
}

func newVSSProcessor(lgr *service.Logger) *vssProcessor {
	return &vssProcessor{
		logger: lgr,
	}
}

func (*vssProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to extract message bytes: %w", err)
	}
	vehicle, err := vss.FromData(msgBytes, true)
	if err != nil {
		return nil, fmt.Errorf("failed to decode vehicle from JSON: %w", err)
	}
	valSlice := vss.VehicleToSlice(vehicle)
	msg.SetStructured(valSlice)
	return []*service.Message{msg}, nil
}

// Close does nothing because our processor doesn't need to clean up resources.
func (*vssProcessor) Close(context.Context) error {
	return nil
}
