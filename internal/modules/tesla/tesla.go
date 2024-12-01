package tesla

import (
	"context"

	"github.com/DIMO-Network/model-garage/pkg/tesla/status"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Module holds dependencies for the Tesla module. At present, there are none.
type Module struct {
}

// New creates a new Tesla, uninitialized module.
func New() (*Module, error) {
	return &Module{}, nil
}

// SetLogger sets the logger for the module.
func (m *Module) SetLogger(_ *service.Logger) {
}

// SetConfig sets the configuration for the module.
func (m *Module) SetConfig(_ string) error {
	return nil
}

// SignalConvert converts a Tesla CloudEvent to DIMO's VSS rows.
func (m *Module) SignalConvert(_ context.Context, msgBytes []byte) ([]vss.Signal, error) {
	return status.Decode(msgBytes)
}

// CloudEventConvert converts an input message to Cloud Events. In the Tesla case
// there is no conversion to perform.
func (m Module) CloudEventConvert(_ context.Context, msgData []byte) ([]byte, error) {
	return msgData, nil
}
