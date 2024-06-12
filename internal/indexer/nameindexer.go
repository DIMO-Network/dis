package indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/DIMO-Network/nameindexer"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/ethereum/go-ethereum/common"
)

const (
	pluginName = "name_indexer"
)

var configSpec = service.NewConfigSpec().
	Summary("Create an indexable string from provided Bloblang parameters.").
	Field(service.NewInterpolatedStringField("timestamp").Description("Timestamp for the index").Default("now()")).
	Field(service.NewInterpolatedStringField("primary_filler").Description("Primary filler for the index").Default("MM")).
	Field(service.NewInterpolatedStringField("secondary_filler").Description("Secondary filler for the index").Default("00")).
	Field(service.NewInterpolatedStringField("data_type").Description("Data type for the index").Default("FP/v0.0.1")).
	Field(service.NewInterpolatedStringField("address").Description("Ethereum address for the index"))

func init() {
	err := service.RegisterProcessor(pluginName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
	timestamp, err := conf.FieldInterpolatedString("timestamp")
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp field: %w", err)
	}
	primaryFiller, err := conf.FieldInterpolatedString("primary_filler")
	if err != nil {
		return nil, fmt.Errorf("failed to parse primary filler field: %w", err)
	}
	secondaryFiller, err := conf.FieldInterpolatedString("secondary_filler")
	if err != nil {
		return nil, fmt.Errorf("failed to parse secondary filler field: %w", err)
	}
	dataType, err := conf.FieldInterpolatedString("data_type")
	if err != nil {
		return nil, fmt.Errorf("failed to parse data type field: %w", err)
	}
	address, err := conf.FieldInterpolatedString("address")
	if err != nil {
		return nil, fmt.Errorf("failed to parse address field: %w", err)
	}

	return &Processor{
		timestamp:       timestamp,
		primaryFiller:   primaryFiller,
		secondaryFiller: secondaryFiller,
		dataType:        dataType,
		address:         address,
	}, nil
}

type Processor struct {
	timestamp       *service.InterpolatedString
	primaryFiller   *service.InterpolatedString
	secondaryFiller *service.InterpolatedString
	dataType        *service.InterpolatedString
	address         *service.InterpolatedString
}

// Process creates an indexable string from the provided parameters and adds it to the message metadata.
func (p *Processor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Evaluate Bloblang expressions using TryString to handle errors
	timestampStr, err := p.timestamp.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate timestamp: %w", err)
	}
	primaryFiller, err := p.primaryFiller.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate primary filler: %w", err)
	}
	secondaryFiller, err := p.secondaryFiller.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate secondary filler: %w", err)
	}
	dataType, err := p.dataType.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate data type: %w", err)
	}
	addressStr, err := p.address.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate address: %w", err)
	}

	// Parse the timestamp
	var timestamp time.Time
	if timestampStr == "now()" {
		timestamp = time.Now()
	} else {
		timestamp, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp format: %w", err)
		}
	}

	// Parse the address
	if addressStr == "null" || addressStr == "" {
		return nil, fmt.Errorf("empty address field")
	}
	address := common.HexToAddress(addressStr)

	// Create the index
	index := nameindexer.Index{
		Timestamp:       timestamp,
		PrimaryFiller:   primaryFiller,
		SecondaryFiller: secondaryFiller,
		DataType:        dataType,
		Address:         address,
	}

	// Encode the index
	encodedIndex, err := nameindexer.EncodeIndex(&index)
	if err != nil {
		return nil, fmt.Errorf("failed to encode index: %w", err)
	}

	// Set the encoded index in the message metadata
	msg.MetaSet("index", encodedIndex)

	return service.MessageBatch{msg}, nil
}

// Close does nothing because our processor doesn't need to clean up resources.
func (*Processor) Close(context.Context) error {
	return nil
}
