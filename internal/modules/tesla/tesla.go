package tesla

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/tesla/status"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/tidwall/gjson"
)

// Module holds dependencies for the Tesla module. At present, there are none.
type Module struct{}

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
func (m Module) CloudEventConvert(_ context.Context, msgData []byte) ([]cloudevent.CloudEventHeader, []byte, error) {
	event := cloudevent.CloudEvent[json.RawMessage]{}
	err := json.Unmarshal(msgData, &event)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	hdrs := []cloudevent.CloudEventHeader{event.CloudEventHeader}
	if gjson.GetBytes(event.Data, "vin").Exists() {
		fpHdr := event.CloudEventHeader
		fpHdr.Type = cloudevent.TypeFingerprint
		hdrs = append(hdrs, fpHdr)
	}

	return hdrs, event.Data, nil
}
