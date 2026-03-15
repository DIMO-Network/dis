package cloudeventsizeroute

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName = "dimo_cloudevent_size_route"

	defaultSizeMetadataKey  = "dimo_raw_ce_size_bytes"
	defaultRouteMetadataKey = "dimo_is_large_cloudevent"
)

var configSpec = service.NewConfigSpec().
	Summary("Classifies messages by raw byte size and stores the result in metadata.").
	Field(service.NewIntField("threshold_bytes").Default(1048576).Description("Messages larger than this size are marked as large.")).
	Field(service.NewStringField("size_metadata_key").Default(defaultSizeMetadataKey).Description("Metadata key to store the raw message size in bytes.")).
	Field(service.NewStringField("route_metadata_key").Default(defaultRouteMetadataKey).Description("Metadata key to store whether the message is large."))

func init() {
	if err := service.RegisterProcessor(processorName, configSpec, ctor); err != nil {
		panic(err)
	}
}

type processor struct {
	threshold        int
	sizeMetadataKey  string
	routeMetadataKey string
}

func ctor(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
	threshold, err := conf.FieldInt("threshold_bytes")
	if err != nil {
		return nil, fmt.Errorf("threshold_bytes: %w", err)
	}
	sizeMetadataKey, err := conf.FieldString("size_metadata_key")
	if err != nil {
		return nil, fmt.Errorf("size_metadata_key: %w", err)
	}
	routeMetadataKey, err := conf.FieldString("route_metadata_key")
	if err != nil {
		return nil, fmt.Errorf("route_metadata_key: %w", err)
	}

	return &processor{
		threshold:        threshold,
		sizeMetadataKey:  sizeMetadataKey,
		routeMetadataKey: routeMetadataKey,
	}, nil
}

func (p *processor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	payload, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("get message bytes: %w", err)
	}

	size := len(payload)
	msg.MetaSetMut(p.sizeMetadataKey, strconv.Itoa(size))
	msg.MetaSetMut(p.routeMetadataKey, strconv.FormatBool(size > p.threshold))

	return service.MessageBatch{msg}, nil
}

func (p *processor) Close(context.Context) error {
	return nil
}
