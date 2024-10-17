package dbmigration

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DIMO-Network/clickhouse-infra/pkg/migrate"
	sigmigrations "github.com/DIMO-Network/model-garage/pkg/migrations"
	indexmigrations "github.com/DIMO-Network/nameindexer/pkg/clickhouse/migrations"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	indexMigrationProcName  = "dimo_file_index_migration"
	signalMigrationProcName = "dimo_signal_migration"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts events into a list of signals").
	Field(service.NewStringField("dsn").Description("DSN connection string for database where migration should be run."))

func init() {
	Register(indexMigrationProcName)
	Register(signalMigrationProcName)
}

// Register registers the processor with the service.
func Register(procName string) {
	err := service.RegisterBatchProcessor(procName, configSpec, ctor(procName))
	if err != nil {
		panic(err)
	}
}

func ctor(procName string) func(*service.ParsedConfig, *service.Resources) (service.BatchProcessor, error) {
	return func(cfg *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
		migration, err := cfg.FieldString("dsn")
		if err != nil {
			return nil, fmt.Errorf("failed to parse migration field: %w", err)
		}
		var registerFunc []func()
		switch procName {
		case indexMigrationProcName:
			registerFunc = indexmigrations.RegisterFuncs()
		case signalMigrationProcName:
			registerFunc = sigmigrations.RegisterFuncs()
		}

		if err := runMigration(migration, registerFunc); err != nil {
			return nil, fmt.Errorf("failed to run migration: %w", err)
		}

		return noop{
			procName: procName,
		}, nil
	}
}

func runMigration(dsn string, registeredFuncs []func()) error {
	dbOptions, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse dsn: %w", err)
	}
	db := clickhouse.OpenDB(dbOptions)
	err = migrate.RunGoose(context.Background(), []string{"up", "-v"}, registeredFuncs, db)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to run migration: %w", err)
	}
	err = db.Close()
	if err != nil {
		return fmt.Errorf("failed to close db: %w", err)
	}
	return nil
}

type noop struct {
	procName string
}

// Close to fulfill the service.Processor interface.
func (noop) Close(context.Context) error { return nil }

// ProcessBatch to fulfill the service.Processor interface.
func (noop) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	return []service.MessageBatch{msgs}, nil
}
