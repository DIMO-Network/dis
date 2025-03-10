package defaultmodule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/schema"
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
	// Value is the value of the signal collected. If the signal base type is a number it will be converted to a float64.
	Value any `json:"value"`
}

// Module holds dependencies for the default module. At present, there are none.
type Module struct {
	signalMap map[string]*schema.SignalInfo
}

// New creates a new default, uninitialized module.
func New() (*Module, error) {
	defs, err := schema.LoadDefinitionFile(strings.NewReader(schema.DefaultDefinitionsYAML()))
	if err != nil {
		return nil, fmt.Errorf("failed to load default schema definitions: %w", err)
	}
	signalInfo, err := schema.LoadSignalsCSV(strings.NewReader(schema.VssRel42DIMO()))
	if err != nil {
		return nil, fmt.Errorf("failed to load default signal info: %w", err)
	}
	definedSignals := defs.DefinedSignal(signalInfo)
	signalMap := make(map[string]*schema.SignalInfo, len(definedSignals))
	for _, signal := range definedSignals {
		signalMap[signal.JSONName] = signal
	}

	return &Module{signalMap: signalMap}, nil
}

// SetLogger sets the logger for the module.
func (Module) SetLogger(*service.Logger) {}

// SetConfig sets the configuration for the module.
func (Module) SetConfig(string) error { return nil }

// SignalConvert converts a default CloudEvent to DIMO's vss signals.
func (m *Module) SignalConvert(_ context.Context, msgBytes []byte) ([]vss.Signal, error) {
	signalEvent := cloudevent.CloudEvent[SignalData]{}
	err := json.Unmarshal(msgBytes, &signalEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal signal data: %w", err)
	}
	var decodeErrs error
	vssSignals := make([]vss.Signal, 0)
	for _, signal := range signalEvent.Data.Signals {
		vssSig, err := m.defaultSignalToVSS(signal)
		if err != nil {
			// we want to return decoded signals even if some fail
			decodeErrs = errors.Join(decodeErrs, err)
			continue
		}
		vssSignals = append(vssSignals, vssSig)
	}
	return vssSignals, decodeErrs
}

func (m *Module) defaultSignalToVSS(signal *Signal) (vss.Signal, error) {
	signalInfo, ok := m.signalMap[signal.Name]
	if !ok {
		return vss.Signal{}, fmt.Errorf("signal %s is not a defined signal name", signal.Name)
	}
	if signal.Value == nil {
		return vss.Signal{}, fmt.Errorf("signal %s is missing a value", signal.Name)
	}
	vssSig := vss.Signal{
		Timestamp: signal.Timestamp,
		Name:      signal.Name,
	}
	switch signalInfo.BaseGoType {
	case "float64":
		num, ok := signal.Value.(float64)
		if ok {
			vssSig.ValueNumber = num
		} else if str, ok := signal.Value.(string); ok {
			v, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return vss.Signal{}, fmt.Errorf("signal %s can not be converted to a float64: %w", signal.Name, err)
			}
			vssSig.ValueNumber = v
		} else {
			return vss.Signal{}, fmt.Errorf("signal %s is not a float64", signal.Name)
		}
	case "string":
		str, ok := signal.Value.(string)
		if !ok {
			return vss.Signal{}, fmt.Errorf("signal %s is not a string", signal.Name)
		}
		vssSig.ValueString = str
	default:
		return vss.Signal{}, fmt.Errorf("signal %s has an unsupported base type %s", signal.Name, signalInfo.BaseGoType)
	}

	return vssSig, nil
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
