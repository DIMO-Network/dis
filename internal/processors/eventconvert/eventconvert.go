package eventconvert

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	eventCategory = "eventCategory"
)

var (
	validCharacters = regexp.MustCompile(`^[a-zA-Z]+$`)
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

	var evtData EventData
	if err := json.Unmarshal(event.Data, &evtData); err != nil {
		processors.SetError(msg, processorName, "failed to parse event category data", err)
		return batch
	}

	if evtData.EventCategory == "" || !validCharacters.MatchString(evtData.EventCategory) {
		processors.SetError(msg, processorName, "invalid event category", fmt.Errorf("missing or invalid event category: %s", evtData.EventCategory))
		return batch
	}

	event.Extras = map[string]any{
		eventCategory: evtData.EventCategory,
	}

	return batch
}

type EventData struct {
	EventCategory string `json:"eventCategory"`
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

	// event producer is device
	producerDID, err := cloudevent.DecodeERC721DID(event.Producer)
	if err != nil {
		return fmt.Errorf("failed to decode producer did: %w", err)
	}

	if producerDID.ChainID != v.chainID {
		return fmt.Errorf("expected producer did chain id %d; recieved: %d", v.chainID, producerDID.ChainID)
	}

	if producerDID.ContractAddress.Cmp(v.vehicleNFTAddress) != 0 {
		return fmt.Errorf("expected producer did contract address %s; recieved: %s", v.vehicleNFTAddress.Hex(), producerDID.ContractAddress.Hex())
	}

	// source must be a valid hex addr
	if !common.IsHexAddress(event.Source) {
		return fmt.Errorf("invalid source. must be valid hex address: %s", event.Source)
	}

	return nil
}
