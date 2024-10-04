package ruptela

import (
	"context"
	"errors"
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/ruptela"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Module is a module that converts ruptela messages to signals.
type Module struct {
	logger *service.Logger
}

// New creates a new Module.
func New() (*Module, error) {
	return &Module{}, nil
}

// SetLogger sets the logger for the module.
func (m *Module) SetLogger(logger *service.Logger) {
	m.logger = logger
}

// SetConfig sets the configuration for the module.
func (m *Module) SetConfig(config string) error {
	return nil
}

// SignalConvert converts a message to signals.
func (m Module) SignalConvert(ctx context.Context, msgBytes []byte) ([]vss.Signal, error) {
	signals, err := ruptela.SignalsFromV1Payload(msgBytes)
	if err == nil {
		return signals, nil
	}

	convertErr := convert.ConversionError{}
	if !errors.As(err, &convertErr) {
		// Add the error to the batch and continue to the next message.
		return nil, fmt.Errorf("failed to convert signals: %w", err)
	}

	return convertErr.DecodedSignals, convertErr
}

// CloudEventConvert converts a message to cloud events.
func (Module) CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error) {
	return [][]byte{msgData}, nil
}
