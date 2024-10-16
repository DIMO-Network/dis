package cloudeventconvert

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName = "dimo_cloudevent_index"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts events into a list of signals")

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &eventIndexProcessor{
		logger: mgr.Logger(),
	}, nil
}
