package fingerprintvalidate

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName = "dimo_validate_fingerprint"
)

var configSpec = service.NewConfigSpec().
	Summary("vaildates fingerprint provides a valid VIN")

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	mgr.Metrics()
	return &processor{
		logger: mgr.Logger(),
	}, nil
}
