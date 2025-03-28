package fingerprintvalidate

import (
	"context"
	"regexp"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var vinRegex = regexp.MustCompile(`^[A-HJ-NPR-Z0-9]{17}$`)

type processor struct {
	logger *service.Logger
}

// Close to fulfill the service.Processor interface.
func (*processor) Close(context.Context) error {
	return nil
}

// ProcessBatch to fulfill the service.BatchProcessor interface.
func (v *processor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		retBatches = append(retBatches, v.processMsg(ctx, msg))
	}
	return retBatches, nil
}

func (v *processor) processMsg(ctx context.Context, msg *service.Message) service.MessageBatch {
	// always return the original message
	batch := service.MessageBatch{msg}
	rawEvent, err := processors.MsgToEvent(msg)
	if err != nil {
		processors.SetError(msg, processorName, "failed to convert to event", err)
		return batch
	}
	if rawEvent.Type != cloudevent.TypeFingerprint {
		return batch
	}
	fingerprint, err := modules.ConvertToFingerprint(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		// Add the error to the batch and continue to the next message.
		processors.SetError(msg, processorName, "failed to convert to fingerprint", err)
		return batch
	}
	if !vinRegex.MatchString(fingerprint.VIN) {
		processors.SetError(msg, processorName, "invalid VIN in fingerprint", err)
		return batch
	}
	return batch
}
