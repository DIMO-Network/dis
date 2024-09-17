package signalconvert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DIMO-Network/DIS/internal/service/deviceapi"
	"github.com/DIMO-Network/model-garage/pkg/migrations"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	"github.com/pressly/goose"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/mod/semver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type vssProcessor struct {
	logger      *service.Logger
	tokenGetter convert.TokenIDGetter
}

// Close to fulfill the service.Processor interface.
func (*vssProcessor) Close(context.Context) error {
	return nil
}

func newVSSProcessor(lgr *service.Logger, devicesAPIGRPCAddr string) (*vssProcessor, error) {
	devicesConn, err := grpc.NewClient(devicesAPIGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial devices api: %w", err)
	}
	deviceAPI := deviceapi.NewService(devicesConn)
	limitedDeviceAPI := NewLimitedTokenGetter(deviceAPI, lgr)
	return &vssProcessor{
		logger:      lgr,
		tokenGetter: limitedDeviceAPI,
	}, nil
}

func (v *vssProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		partialErr := msg.Copy()
		// Get the JSON message and convert it to a DIMO status.
		msgBytes, err := msg.AsBytes()
		if err != nil {
			// Add the error to the batch and continue to the next message.
			partialErr.SetError(fmt.Errorf("failed to extract message bytes: %w", err))
			retBatches = append(retBatches, service.MessageBatch{partialErr})
			continue
		}
		schemaVersion := convert.GetSchemaVersion(msgBytes)
		if semver.Compare(convert.StatusV1Converted, schemaVersion) == 0 {
			// ignore v1.1 messages
			continue
		}
		var retBatch service.MessageBatch
		signals, err := convert.SignalsFromPayload(ctx, v.tokenGetter, msgBytes)
		if err != nil {
			if errors.As(err, &deviceapi.NotFoundError{}) {
				// If we do not have an Token for this device we want to drop the message. But we don't want to log an error.
				v.logger.Trace(fmt.Sprintf("dropping message: %v", err))
				continue
			}

			convertErr := convert.ConversionError{}
			if !errors.As(err, &convertErr) {
				// Add the error to the batch and continue to the next message.
				partialErr.SetError(fmt.Errorf("failed to convert signals: %w", err))
				retBatches = append(retBatches, service.MessageBatch{partialErr})
				continue
			}

			// If we have a conversion error we will add a error message with metadata to the batch,
			// but still return the signals that we could decode.
			partialErr.SetError(err)
			data, err := json.Marshal(convertErr)
			if err == nil {
				partialErr.SetBytes(data)
			}
			retBatch = append(retBatch, partialErr)
			signals = convertErr.DecodedSignals
		}

		for i := range signals {
			sigVals := vss.SignalToSlice(signals[i])
			msgCpy := msg.Copy()
			msgCpy.SetStructured(sigVals)
			retBatch = append(retBatch, msgCpy)
		}
		retBatches = append(retBatches, retBatch)
	}
	return retBatches, nil
}

func runMigration(dsn string) error {
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
