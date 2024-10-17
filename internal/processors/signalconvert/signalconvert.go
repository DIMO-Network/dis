package signalconvert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/dis/internal/modules"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type SignalModule interface {
	SignalConvert(ctx context.Context, msgData []byte) ([]vss.Signal, error)
}
type vssProcessor struct {
	signalModule SignalModule
	Logger       *service.Logger
}

// Close to fulfill the service.Processor interface.
func (*vssProcessor) Close(context.Context) error {
	return nil
}

func newVSSProcessor(lgr *service.Logger, moduleName, moduleConfig string) (*vssProcessor, error) {
	decodedModuelConfig, err := base64.StdEncoding.DecodeString(moduleConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decode module config: %w", err)
	}
	moduleOpts := modules.Options{
		Logger:       lgr,
		FilePath:     "",
		ModuleConfig: string(decodedModuelConfig),
	}
	signalModule, err := modules.LoadSignalModule(moduleName, moduleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to load signal module: %w", err)
	}
	return &vssProcessor{
		signalModule: signalModule,
		Logger:       lgr,
	}, nil
}

func (v *vssProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
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
		signals, err := v.signalModule.SignalConvert(ctx, msgBytes)
		if err != nil {
			errMsg.SetError(err)
			data, err := json.Marshal(err)
			if err == nil {
				errMsg.SetBytes(data)
			}
			retBatch = append(retBatch, errMsg)
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
