package indexer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/nameindexer"
	chindexer "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/DIMO-Network/nameindexer/pkg/clickhouse/migrations"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pressly/goose"
)

const pluginName = "name_indexer"

// Configuration specification for the processor.
var configSpec = service.NewConfigSpec().
	Summary("Create an indexable string from provided Bloblang parameters.").
	Field(service.NewInterpolatedStringField("timestamp").Description("Timestamp for the index")).
	Field(service.NewInterpolatedStringField("primary_filler").Description("Primary filler for the index").Default("MM")).
	Field(service.NewInterpolatedStringField("secondary_filler").Description("Secondary filler for the index").Default("00")).
	Field(service.NewInterpolatedStringField("data_type").Description("Data type for the index").Default("FP/v0.0.1")).
	Field(service.NewObjectField("subject",
		service.NewInterpolatedStringField("address").Description("Ethereum address for the index").Optional(),
		service.NewInterpolatedStringField("token_id").Description("Token Id for the index").Optional(),
	)).
	Field(service.NewStringField("migration").Default("").Description("DSN connection string for database where migration should be run. If set, the plugin will run a database migration on startup using the provided DNS string."))

func init() {
	if err := service.RegisterProcessor(pluginName, configSpec, ctor); err != nil {
		panic(err)
	}
}

// Processor is a processor that creates an indexable string from the provided parameters.
type Processor struct {
	timestamp       *service.InterpolatedString
	primaryFiller   *service.InterpolatedString
	secondaryFiller *service.InterpolatedString
	dataType        *service.InterpolatedString
	subject         *subjectInterpolatedString
}

type subjectInterpolatedString struct {
	interpolatedString *service.InterpolatedString
	isAddress          bool
}

// TryIndexSubject evaluates the subject field and returns a nameindexer.Subject.
// The subject field can be either an address or a token_id.
func (s *subjectInterpolatedString) TryIndexSubject(msg *service.Message) (nameindexer.Subject, error) {
	subjectStr, err := s.interpolatedString.TryString(msg)
	if err != nil {
		return nameindexer.Subject{}, fmt.Errorf("failed to evaluate subject: %w", err)
	}

	if s.isAddress {
		if !common.IsHexAddress(subjectStr) {
			return nameindexer.Subject{}, fmt.Errorf("address is not a valid hexadecimal address: %s", subjectStr)
		}
		addr := common.HexToAddress(subjectStr)
		return nameindexer.Subject{
			Address: &addr,
		}, nil
	}

	tokenID, err := strconv.ParseUint(subjectStr, 10, 32)
	if err != nil {
		return nameindexer.Subject{}, fmt.Errorf("failed to parse token_id: %w", err)
	}
	tokenID32 := uint32(tokenID)
	return nameindexer.Subject{
		TokenID: &tokenID32,
	}, nil
}

// Constructor for the Processor.
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

	subject, err := getSubject(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse subject field: %w", err)
	}

	migration, err := conf.FieldString("migration")
	if err != nil {
		return nil, fmt.Errorf("failed to parse migration field: %w", err)
	}
	if migration != "" {
		if err := runMigration(migration); err != nil {
			return nil, fmt.Errorf("failed to run migration: %w", err)
		}
	}

	return &Processor{
		timestamp:       timestamp,
		primaryFiller:   primaryFiller,
		secondaryFiller: secondaryFiller,
		dataType:        dataType,
		subject:         subject,
	}, nil
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
	timestamp, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp format: %w", err)
	}

	idxSubject, err := p.subject.TryIndexSubject(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate subject: %w", err)
	}

	// Create the index
	index := nameindexer.Index{
		Timestamp:       timestamp,
		PrimaryFiller:   primaryFiller,
		SecondaryFiller: secondaryFiller,
		DataType:        dataType,
		Subject:         idxSubject,
	}

	// Encode the index
	encodedIndex, err := nameindexer.EncodeIndex(&index)
	if err != nil {
		return nil, fmt.Errorf("failed to encode index: %w", err)
	}

	// Set the encoded index in the message metadata
	msg.MetaSetMut("index", encodedIndex)
	indexValues, err := chindexer.IndexToSlice(&index)
	if err != nil {
		return nil, fmt.Errorf("failed to convert index to slice: %w", err)
	}
	msg.MetaSetMut("index_values", indexValues)

	return service.MessageBatch{msg}, nil
}

// Close does nothing because our processor doesn't need to clean up resources.
func (*Processor) Close(context.Context) error {
	return nil
}

// getSubject parses the subject field from the configuration.
func getSubject(config *service.ParsedConfig) (*subjectInterpolatedString, error) {
	subConfig := config.Namespace("subject")
	addrSet := subConfig.Contains("address")
	tokenIDSet := subConfig.Contains("token_id")
	if addrSet && tokenIDSet || !addrSet && !tokenIDSet {
		return nil, fmt.Errorf("either address or token_id must be set as the subject")
	}
	if addrSet {
		interpolatedString, err := subConfig.FieldInterpolatedString("address")
		if err != nil {
			return nil, fmt.Errorf("failed to parse address field: %w", err)
		}
		return &subjectInterpolatedString{
			interpolatedString: interpolatedString,
			isAddress:          true,
		}, nil
	}
	interpolatedString, err := subConfig.FieldInterpolatedString("token_id")
	if err != nil {
		return nil, fmt.Errorf("failed to parse token_id field: %w", err)
	}
	return &subjectInterpolatedString{
		interpolatedString: interpolatedString,
		isAddress:          false,
	}, nil
}

func runMigration(dsn string) error {
	db, err := goose.OpenDBWithDriver("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	err = migrations.RunGoose(context.Background(), []string{"up", "-v"}, db)
	if err != nil {
		return fmt.Errorf("failed to run migration: %w", err)
	}
	return nil
}
