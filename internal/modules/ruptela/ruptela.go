package ruptela

import (
	"context"
	"errors"

	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Module is a module that converts ruptela messages to signals.
type Module struct {
	TokenGetter convert.TokenIDGetter
	logger      *service.Logger
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
	return nil, errors.New("ruptela signal conversion not implemented")
}

// CloudEventConvert converts a message to cloud events.
func (Module) CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error) {
	return [][]byte{msgData}, nil
}
