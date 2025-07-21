package eventconvert

import (
	"context"
	"encoding/json"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	eventValidContentType = "dimo_valid_event"
)

type eventsProcessor struct {
	logger *service.Logger
}

// Close to fulfill the service.Processor interface.
func (*eventsProcessor) Close(context.Context) error {
	return nil
}

// ProcessBatch to fulfill the service.BatchProcessor interface.
func (e *eventsProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		retBatches = append(retBatches, e.processMsg(ctx, msg))
	}
	return retBatches, nil
}

// processMsg processes a single message and returns a batch of events and or errors.
func (e *eventsProcessor) processMsg(ctx context.Context, msg *service.Message) service.MessageBatch {
	// keep the original message and add any new event messages to the batch
	retBatch := service.MessageBatch{msg}
	rawEvent, err := processors.MsgToEvent(msg)
	if err != nil || !e.isVehicleEventMessage(rawEvent) {
		// leave the message as is and continue to the next message
		return retBatch
	}

	events, partialErr := modules.ConvertToEvents(ctx, rawEvent.Source, *rawEvent)
	if partialErr != nil {
		errMsg := msg.Copy()
		errMsg.SetError(partialErr)
		data, err := json.Marshal(partialErr)
		if err == nil {
			errMsg.SetBytes(data)
		}
		retBatch = append(retBatch, errMsg)
	}

	for i := range events {
		msgCpy := msg.Copy()
		setMetaData(&events[i], rawEvent)
		msgCpy.SetStructured(events[i])
		msgCpy.MetaSetMut(processors.MessageContentKey, eventValidContentType)
		retBatch = append(retBatch, msgCpy)
	}
	return retBatch
}

func setMetaData(event *vss.Event, rawEvent *cloudevent.RawEvent) {
	event.Subject = rawEvent.Subject
	event.Source = rawEvent.Source
	event.Producer = rawEvent.Producer
	event.CloudEventID = rawEvent.ID
}

func (e *eventsProcessor) isVehicleEventMessage(rawEvent *cloudevent.RawEvent) bool {
	return rawEvent.Type == cloudevent.TypeEvent
}
