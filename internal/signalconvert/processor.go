package signalconvert

import (
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName       = "dimo_signal_convert"
	grpcFieldName       = "devices_api_grpc_addr"
	migrationFieldName  = "init_migration"
	moduleNameFieldName = "module_name"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts events into a list of signals").
	Field(service.NewStringField(grpcFieldName).
		Description("DSN connection string for database where migration should be run. If set, the plugin will run a database migration on startup using the provided DNS string.")).
	Field(service.NewStringField(migrationFieldName).Default("").Description("Primary filler for the index").Default("MM")).
	Field(service.NewStringField(moduleNameFieldName).Description("Name of the module to use for decoding.").Default("MM"))

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	grpcAddr, err := cfg.FieldString(grpcFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get grpc address: %w", err)
	}

	dsn, err := cfg.FieldString(migrationFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get dsn: %w", err)
	}
	if dsn != "" {
		err = runMigration(dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to run migration: %w", err)
		}
	}

	return newVSSProcessor(mgr.Logger(), grpcAddr)
}
