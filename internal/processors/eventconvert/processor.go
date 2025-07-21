package eventconvert

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName = "dimo_event_convert"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts a cloud event into a list of events")

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &eventsProcessor{
		logger: mgr.Logger(),
	}, nil
}
