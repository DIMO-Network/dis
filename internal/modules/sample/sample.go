package sample

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type moduleConfig struct {
	SettingA string `json:"settingA"`
	SettingB string `json:"settingB"`
}

// sampleModule is a module that converts sample messages to signals.
type sampleModule struct {
	settings moduleConfig
	logger   *service.Logger
}

// New creates a new sampleModule.
func New() (*sampleModule, error) {
	return &sampleModule{}, nil
}

// SetLogger sets the logger for the module.
func (m *sampleModule) SetLogger(logger *service.Logger) {
	m.logger = logger
}

// SetConfig sets the configuration for the module.
func (m *sampleModule) SetConfig(config string) error {
	var cfg moduleConfig
	err := json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	m.settings = cfg
	return nil
}

// SignalConvert converts a message to signals.
func (m sampleModule) SignalConvert(ctx context.Context, msgBytes []byte) ([]vss.Signal, error) {
	// "tokenId":123,"timestamp":"2024-04-18T17:20:26.633Z","name":"powertrainCombustionEngineECT","valueNumber":107,"valueString":"","source":"0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF"}
	signals := []vss.Signal{
		{
			TokenID:     123,
			Timestamp:   time.Date(2024, 4, 18, 17, 20, 26, 633000000, time.UTC),
			Name:        "powertrainCombustionEngineECT",
			ValueNumber: 107,
			Source:      "0xSampleIntegrationAddr",
		},
	}
	return signals, nil
}

// CloudEventConvert converts a sample message to cloud events.
func (sampleModule) CloudEventConvert(_ context.Context, msgData []byte) ([]cloudevent.CloudEventHeader, []byte, error) {
	event := cloudevent.CloudEvent[json.RawMessage]{}
	err := json.Unmarshal(msgData, &event)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return []cloudevent.CloudEventHeader{event.CloudEventHeader}, event.Data, nil
}
