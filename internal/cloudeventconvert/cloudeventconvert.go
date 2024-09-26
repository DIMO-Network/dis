package cloudeventconvert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/dis/internal/modules"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type CloudEventModule interface {
	CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error)
}
type cloudeventProcessor struct {
	cloudEventModule CloudEventModule
}

// Close to fulfill the service.Processor interface.
func (*cloudeventProcessor) Close(context.Context) error {
	return nil
}

func newCloudConvertProcessor(lgr *service.Logger, moduleName, moduleConfig string) (*cloudeventProcessor, error) {
	decodedModuelConfig, err := base64.StdEncoding.DecodeString(moduleConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decode module config: %w", err)
	}
	moduleOpts := modules.Options{
		Logger:       lgr,
		FilePath:     "",
		ModuleConfig: string(decodedModuelConfig),
	}
	cloudEventModule, err := modules.LoadCloudEventModule(moduleName, moduleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to load signal module: %w", err)
	}
	return &cloudeventProcessor{
		cloudEventModule: cloudEventModule,
	}, nil
}

func (v *cloudeventProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		var retBatch service.MessageBatch
		errMsg := msg.Copy()
		msgBytes, err := msg.AsBytes()
		if err != nil {
			// Add the error to the batch and continue to the next message.
			errMsg.SetError(fmt.Errorf("failed to get msg bytes: %w", err))
			retBatches = append(retBatches, service.MessageBatch{errMsg})
			continue
		}

		events, err := v.cloudEventModule.CloudEventConvert(ctx, msgBytes)
		if err != nil {
			errMsg.SetError(err)
			data, err := json.Marshal(err)
			if err == nil {
				errMsg.SetBytes(data)
			}
			retBatch = append(retBatch, errMsg)
		}
		for _, event := range events {
			msgCpy := msg.Copy()
			msgCpy.SetBytes(event)
			retBatch = append(retBatch, msgCpy)
		}
		retBatches = append(retBatches, retBatch)
	}
	return retBatches, nil
}
