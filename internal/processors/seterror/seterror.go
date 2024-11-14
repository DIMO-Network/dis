package seterror

import (
	"context"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var configSpec = service.NewConfigSpec().
	Summary("Set's a message as errored with a given message").
	Field(service.NewInterpolatedStringField("message").Description("Message to use in the error.").Optional().Default("error"))

func init() {
	err := service.RegisterBatchProcessor("set_error", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	message, err := cfg.FieldInterpolatedString("message")
	if err != nil {
		return nil, fmt.Errorf("failed to parse message field: %w", err)
	}
	return processor{
		messageField: message,
	}, nil

}

type processor struct {
	messageField *service.InterpolatedString
}

// Close to fulfill the service.Processor interface.
func (processor) Close(context.Context) error { return nil }

// ProcessBatch to fulfill the service.Processor sets an error on each message.
func (p processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	for _, msg := range msgs {
		errMsg, err := p.messageField.TryString(msg)
		if err != nil {
			msg.SetError(fmt.Errorf("failed to interpolate message: %w", err))
			continue
		}
		msg.SetError(errors.New(errMsg))
	}
	return []service.MessageBatch{msgs}, nil
}
