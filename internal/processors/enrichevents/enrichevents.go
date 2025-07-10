package enrichevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/cloudeventconvert"
	"github.com/DIMO-Network/model-garage/pkg/occurrences"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type processor struct {
	logger            *service.Logger
	vehicleNFTAddress common.Address
	chainID           uint64
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

func (v *processor) processMsg(_ context.Context, msg *service.Message) service.MessageBatch {
	batch := service.MessageBatch{msg}
	event, err := processors.MsgToEvent(msg)
	if err != nil || event.Type != cloudevent.TypeEvent {
		return batch
	}

	if err := v.ValidateEvent(event); err != nil {
		processors.SetError(msg, processorName, "failed to validate event", err)
		return batch
	}

	var evts Events
	if err := json.Unmarshal(event.Data, &evts); err != nil {
		processors.SetError(msg, processorName, "failed to parse event specific data", err)
		return batch
	}

	for _, evt := range evts.Events {
		if evt.Name == "" || !cloudeventconvert.ValidIdentifier(evt.Name) {
			processors.SetError(msg, processorName, "invalid event category", fmt.Errorf("missing or invalid event name: %q", evt.Name))
			batch = append(batch, msg)
			continue
		}

		var storedEvt occurrences.Event
		if err := v.validateAndFormatEvent(event.CloudEventHeader, evt, &storedEvt); err != nil {
			processors.SetError(msg, processorName, "failed to format event object for storage", err)
			continue
		}

		msgCpy := msg.Copy()
		msgCpy.SetStructured(storedEvt)
		batch = append(batch, msgCpy)

	}

	return batch
}

type Events struct {
	Events []EventData `json:"events"`
}

type EventData struct {
	Name     string          `json:"name"`
	Time     *string         `json:"time,omitempty"`
	Duration *string         `json:"duration,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

func (v *processor) ValidateEvent(event *cloudevent.RawEvent) error {
	// event subject is the vehicle
	subjDID, err := cloudevent.DecodeERC721DID(event.Subject)
	if err != nil {
		return fmt.Errorf("failed to decode subject did: %w", err)
	}

	if subjDID.ChainID != v.chainID {
		return fmt.Errorf("expected subject did chain id %d; recieved: %d", v.chainID, subjDID.ChainID)
	}

	if subjDID.ContractAddress.Cmp(v.vehicleNFTAddress) != 0 {
		return fmt.Errorf("expected subject did contract address %s; recieved: %s", v.vehicleNFTAddress.Hex(), subjDID.ContractAddress.Hex())
	}

	// source must be a valid hex addr
	if !common.IsHexAddress(event.Source) {
		return fmt.Errorf("invalid source. must be valid hex address: %s", event.Source)
	}

	return nil
}

func (v *processor) validateAndFormatEvent(header cloudevent.CloudEventHeader, event EventData, storedEvtObj *occurrences.Event) error {
	if event.Duration != nil {
		dur, err := time.ParseDuration(*event.Duration)
		if err != nil {
			return fmt.Errorf("invalid duration for event %q: %w", event.Name, err)
		}
		storedEvtObj.EventDuration = dur
	}

	if event.Time != nil {
		t, err := time.Parse(time.RFC3339, *event.Time)
		if err != nil {
			return fmt.Errorf("invalid time for event %q: %w", event.Name, err)
		}
		storedEvtObj.EventTime = t
	}

	storedEvtObj.CloudEventID = header.ID
	storedEvtObj.Subject = header.Subject
	storedEvtObj.Source = header.Source
	storedEvtObj.Producer = header.Producer
	storedEvtObj.EventName = event.Name

	if len(event.Metadata) > 0 {
		var tmp map[string]interface{}
		if err := json.Unmarshal(event.Metadata, &tmp); err != nil {
			return fmt.Errorf("invalid metadata JSON for event %q: %w", event.Name, err)
		}
		storedEvtObj.EventMetaData = string(event.Metadata)
	}

	return nil
}
