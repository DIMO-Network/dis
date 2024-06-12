package fingerprintindexer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/nameindexer"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/ethereum/go-ethereum/common"
)

const (
	pluginName    = "fingerprint_indexer"
	pluginSummary = "Create a indexable string from a DIMO fingerprint message."
)

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec()
	configSpec.Summary(pluginSummary)

	err := service.RegisterProcessor(pluginName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(*service.ParsedConfig, *service.Resources) (service.Processor, error) {
	return &Processor{}, nil
}

type Processor struct{}

type cloudEventHeaders struct {
	Time    time.Time `json:"time"`
	Subject string    `json:"subject"`
}

// Process the message and create a indexable string from the fingerprint message and adds it to the message metadata.
func (*Processor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Get the JSON message and convert it to a DIMO status.
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to extract message bytes: %w", err)
	}

	header := cloudEventHeaders{}
	err = json.Unmarshal(msgBytes, &header)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if header.Time.IsZero() {
		header.Time = time.Now()
	}

	index := nameindexer.Index{
		Timestamp: header.Time,
		Address:   common.HexToAddress(header.Subject),
		DataType:  "FP/v0.0.1",
	}
	encodedIndex, err := nameindexer.EncodeIndex(&index)
	if err != nil {
		return nil, fmt.Errorf("failed to encode index: %w", err)
	}
	msg.MetaSet("index", encodedIndex)
	return service.MessageBatch{msg}, nil
}

// Close does nothing because our processor doesn't need to clean up resources.
func (*Processor) Close(context.Context) error {
	return nil
}
