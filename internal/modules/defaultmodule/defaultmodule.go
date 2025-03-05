package defaultmodule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/tidwall/gjson"
)

// SignalData is a struct for holding vss signal data.
type SignalData struct {
	Signals []*Signal `json:"signals"`
}

// Signal is a struct for holding vss signal data.
type Signal struct {
	// Timestamp is when this data was collected.
	Timestamp time.Time `json:"timestamp"`
	// Name is the name of the signal collected.
	Name string `json:"name"`
	// ValueNumber is the value of the signal collected.
	ValueNumber *float64 `json:"valueNumber"`
	// ValueString is the value of the signal collected.
	ValueString *string `json:"valueString"`
}

// Module holds dependencies for the default module. At present, there are none.
type Module struct{}

// New creates a new default, uninitialized module.
func New() (*Module, error) {
	return &Module{}, nil
}

// SetLogger sets the logger for the module.
func (Module) SetLogger(*service.Logger) {}

// SetConfig sets the configuration for the module.
func (Module) SetConfig(string) error { return nil }

// SignalConvert converts a default CloudEvent to DIMO's vss signals.
func (Module) SignalConvert(_ context.Context, msgBytes []byte) ([]vss.Signal, error) {
	signalEvent := cloudevent.CloudEvent[SignalData]{}
	err := json.Unmarshal(msgBytes, &signalEvent)
	vssSignals := make([]vss.Signal, len(signalEvent.Data.Signals))
	var errs error
	for i, signal := range signalEvent.Data.Signals {
		if signal.ValueNumber != nil && signal.ValueString != nil || signal.ValueNumber == nil && signal.ValueString == nil {
			errs = errors.Join(err, fmt.Errorf("signal %s requires either a valueNumber or valueString but not both", signal.Name))
			continue
		}
		vssSignals[i] = vss.Signal{
			Timestamp: signal.Timestamp,
			Name:      signal.Name,
		}
		if signal.ValueNumber != nil {
			vssSignals[i].ValueNumber = *signal.ValueNumber
		} else if signal.ValueString != nil {
			vssSignals[i].ValueString = *signal.ValueString
		}
	}
	return vssSignals, errs
}

// CloudEventConvert marshals the input message to Cloud Events and sets the type based on the message content.
func (Module) CloudEventConvert(_ context.Context, msgData []byte) ([]cloudevent.CloudEventHeader, []byte, error) {
	var event cloudevent.CloudEvent[json.RawMessage]
	err := json.Unmarshal(msgData, &event)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	hdrs := []cloudevent.CloudEventHeader{}
	if gjson.GetBytes(event.Data, "vin").Exists() {
		fpHdr := event.CloudEventHeader
		fpHdr.Type = cloudevent.TypeFingerprint
		hdrs = append(hdrs, fpHdr)
	}
	if gjson.GetBytes(event.Data, "signals").Exists() {
		statusHdr := event.CloudEventHeader
		statusHdr.Type = cloudevent.TypeStatus
		hdrs = append(hdrs, statusHdr)
	}

	// if we can't infer the type, default to unknown so we don't drop the event.
	if len(hdrs) == 0 {
		unknownHdr := event.CloudEventHeader
		unknownHdr.Type = cloudevent.TypeUnknown
		hdrs = append(hdrs, unknownHdr)
	}

	return hdrs, event.Data, nil
}
