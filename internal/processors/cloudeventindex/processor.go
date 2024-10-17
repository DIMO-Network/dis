package cloudeventconvert

import (
	"time"

	"github.com/DIMO-Network/dis/internal/ratedlogger"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const processorName = "dimo_cloudevent_index"

var configSpec = service.NewConfigSpec().
	Summary("Creates indexes for cloudevents")

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(_ *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &eventIndexProcessor{
		logger: ratedlogger.New(mgr.Logger(), time.Minute*10),
	}, nil
}
