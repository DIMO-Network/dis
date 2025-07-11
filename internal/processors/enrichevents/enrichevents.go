package enrichevents

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/cloudeventconvert"
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
	v.logger.Debug("processing event msg")
	batch := service.MessageBatch{msg}
	event, err := processors.MsgToEvent(msg)
	if err != nil {
		processors.SetError(msg, processorName, "failed to convert to event", err)
		return batch
	}

	if event.Type != cloudevent.TypeEvent {
		return batch
	}

	if err := v.validateEvent(event); err != nil {
		processors.SetError(msg, processorName, "failed to validate event", err)
		return batch
	}

	var evts Events
	if err := json.Unmarshal(event.Data, &evts); err != nil {
		processors.SetError(msg, processorName, "failed to parse event specific data", err)
		return batch
	}

	if event.Extras == nil {
		event.Extras = make(map[string]any)
	}

	for _, evt := range evts.Events {
		if evt.Name == "" || !cloudeventconvert.ValidIdentifier(evt.Name) {
			processors.SetError(msg, processorName, "invalid event category", fmt.Errorf("missing or invalid event category: %q", evt.Name))
			batch = append(batch, msg)
			continue
		}

		if err := v.validateEventQueryFields(evt); err != nil {
			processors.SetError(msg, processorName, "invalid event query fields", err)
			batch = append(batch, msg)
			continue
		}

		event.Extras[evt.Name] = EventExtras{
			Duration: evt.Duration,
			Time:     evt.Time,
		}

		r, err := json.Marshal(event.Extras)
		if err != nil {
			processors.SetError(msg, processorName, "failed to marshal events", err)
		}

		fmt.Println("\n\n", string(r))

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

type EventExtras struct {
	Time     *string `json:"time,omitempty"`
	Duration *string `json:"duration,omitempty"`
}

func (v *processor) validateEvent(event *cloudevent.RawEvent) error {
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

func (v *processor) validateEventQueryFields(event EventData) error {
	if event.Duration != nil {
		if _, err := time.ParseDuration(*event.Duration); err != nil {
			return fmt.Errorf("invalid %s event duration: %w", event.Name, err)
		}
	}

	if event.Time != nil {
		if _, err := time.Parse(time.RFC3339, *event.Time); err != nil {
			return fmt.Errorf("invalid %s event duration: %w", event.Name, err)
		}
	}

	return nil
}
