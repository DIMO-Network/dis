package fingerprintvalidate

import (
	"context"
	"fmt"
	"regexp"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
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
		rawEvent, err := processors.MsgToEvent(msg)
		if err != nil {
			retBatches = processors.AppendError(retBatches, msg, processorName, err)
			continue
		}
		if rawEvent.Type != cloudevent.TypeFingerprint {
			// leave the message as is and continue to the next message
			retBatches = append(retBatches, service.MessageBatch{msg})
			continue
		}
		fingerprint, err := modules.ConvertToFingerprint(ctx, rawEvent.Source, *rawEvent)
		if err != nil {
			retBatches = processors.AppendError(retBatches, msg, processorName, fmt.Errorf("failed to convert to fingerprint: %w", err))
			continue
		}
		if !vinRegex.MatchString(fingerprint.VIN) {
			retBatches = processors.AppendError(retBatches, msg, processorName, fmt.Errorf("invalid VIN in fingerprint: %s", fingerprint.VIN))
			continue
		}
		retBatches = append(retBatches, service.MessageBatch{msg})
	}
	return retBatches, nil
}
