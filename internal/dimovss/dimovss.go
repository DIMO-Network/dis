package dimovss

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/benthos-plugin/internal/service/deviceapi"
	"github.com/DIMO-Network/model-garage/pkg/migrations"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/pressly/goose"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	pluginName         = "vss_vehicle"
	pluginSummary      = "Converts a Status message from a DIMO device into a list of values for insertion into clickhouse."
	grpcFieldName      = "devices_api_grpc_addr"
	grpcFieldDesc      = "The address of the devices API gRPC server."
	migrationFieldName = "init_migration"
)

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	grpcField := service.NewStringField(grpcFieldName)
	grpcField.Description(grpcFieldDesc)
	chConfig := service.NewStringField(migrationFieldName)
	chConfig.Default("")
	chConfig.Description("If set, the plugin will run a database migration on startup. using the provided DNS string.")
	configSpec := service.NewConfigSpec()
	configSpec.Summary(pluginSummary)
	configSpec.Field(grpcField)
	configSpec.Field(chConfig)

	err := service.RegisterProcessor(pluginName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
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

type vssProcessor struct {
	logger    *service.Logger
	deviceSvc *deviceapi.Service
}

func newVSSProcessor(lgr *service.Logger, devicesAPIGRPCAddr string) (*vssProcessor, error) {
	devicesConn, err := grpc.Dial(devicesAPIGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial devices api: %w", err)
	}
	deviceAPI := deviceapi.NewService(devicesConn)

	return &vssProcessor{
		logger:    lgr,
		deviceSvc: deviceAPI,
	}, nil
}

func (v *vssProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Get the JSON message and convert it to a DIMO status.
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to extract message bytes: %w", err)
	}
	dimoStatus, err := vss.FromData(msgBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode dimoStatus from JSON: %w", err)
	}

	// Grab the subject and timestamp f
	valSlice := vss.DimoToSlice(dimoStatus)
	sub := ""
	timeStamp := time.Now().UTC()
	if dimoStatus.Subject != nil {
		sub = *dimoStatus.Subject
	}
	if dimoStatus.Timestamp != nil {
		timeStamp = *dimoStatus.Timestamp
	}

	// create a new message for each signal and one for the dimo status
	// using metadata to differentiate between the two.
	retMsgs := []*service.Message{}

	oldMsg := msg.Copy()
	oldMsg.SetStructured(valSlice)
	oldMsg.MetaSet("vss", "static")
	retMsgs = append(retMsgs, oldMsg)

	tokenID, err := v.deviceSvc.GetTokenIDFromID(ctx, sub)
	if err != nil {
		return retMsgs, fmt.Errorf("failed to get tokenID from deviceAPI: %w", err)
	}

	// Convert the DIMO status to a slice of signals.
	signals := vss.DIMOToSignals(tokenID, timeStamp, valSlice)
	sigVals := make([][]any, len(signals))
	for i := range signals {
		sigVals[i] = vss.SignalToSlice(signals[i])
	}

	for _, signal := range sigVals {
		newMsg := msg.Copy()
		newMsg.SetStructured(signal)
		newMsg.MetaSet("vss", "dynamic")
		retMsgs = append(retMsgs, newMsg)
	}
	return retMsgs, nil
}

// Close does nothing because our processor doesn't need to clean up resources.
func (*vssProcessor) Close(context.Context) error {
	return nil
}

func runMigration(dsn string) error {
	migrations.SetMigrations()
	db, err := goose.OpenDBWithDriver("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	err = migrations.RunGoose(context.Background(), []string{"up", "-v"}, db)
	if err != nil {
		return fmt.Errorf("failed to run migration: %w", err)
	}
	return nil
}
