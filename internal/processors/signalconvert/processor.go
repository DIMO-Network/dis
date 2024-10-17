package signalconvert

import (
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName         = "dimo_signal_convert"
	moduleConfigFieldName = "module_config"
	moduleNameFieldName   = "module_name"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts events into a list of signals").
	Field(service.NewStringField(moduleConfigFieldName).Default("").Description("Optional Configuration that will be passed to the module")).
	Field(service.NewStringField(moduleNameFieldName).Description("Name of the module to use for decoding."))

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	moduleName, err := cfg.FieldString(moduleNameFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get module name: %w", err)
	}

	moduleConfig, err := cfg.FieldString(moduleConfigFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get module config: %w", err)
	}

	return newVSSProcessor(mgr.Logger(), moduleName, moduleConfig)
}
