package signalconvert

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName = "dimo_signal_convert"
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
	return &vssProcessor{
		Logger: mgr.Logger(),
	}, nil
}
