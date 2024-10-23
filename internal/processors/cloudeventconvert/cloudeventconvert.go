package cloudeventconvert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/dis/internal/modules"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/tidwall/sjson"
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
		msgBytes, err := msg.AsBytes()
		if err != nil {
			// Add the error to the batch and continue to the next message.
			retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to get msg bytes: %w", err))
			continue
		}

		events, err := v.cloudEventModule.CloudEventConvert(ctx, msgBytes)
		if err != nil {
			// Try to unmarshal convert errors
			errMsg := msg.Copy()
			errMsg.SetError(err)
			data, marshalErr := json.Marshal(err)
			if marshalErr == nil {
				errMsg.SetBytes(data)
			}
			retBatch = append(retBatch, errMsg)
		}
		for _, event := range events {
			msgCpy := msg.Copy()
			source, ok := msg.MetaGet(httpinputserver.DIMOConnectionIdKey)
			if !ok {
				retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to get source from connection id"))
				break
			}
			var err error
			event, err = setSource(event, source)
			if err != nil {
				retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to set source: %w", err))
				continue
			}
			msgCpy.SetBytes(event)
			retBatch = append(retBatch, msgCpy)
		}
		retBatches = append(retBatches, retBatch)
	}
	return retBatches, nil
}

func setSource(event []byte, source string) ([]byte, error) {
	newEvent, err := sjson.SetBytes(event, "source", source)
	if err != nil {
		return nil, fmt.Errorf("failed to set source: %w", err)
	}
	return newEvent, nil
}
