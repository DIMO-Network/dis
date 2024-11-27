package autopi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/autopi/status"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/ksuid"
)

const (
	StatusEventType      = "com.dimo.device.status.v2"
	FingerprintEventType = "zone.dimo.aftermarket.device.fingerprint"
	DataVersion          = "v2"
)

type moduleConfig struct {
	ChainID                 uint64 `json:"chain_id"`
	AftermarketContractAddr string `json:"aftermarket_contract_addr"`
	VehicleContractAddr     string `json:"vehicle_contract_addr"`
}

// Module is a module that converts autopi messages to signals.
type Module struct {
	logger *service.Logger
	cfg    moduleConfig
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
	err := json.Unmarshal([]byte(config), &m.cfg)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if !common.IsHexAddress(m.cfg.AftermarketContractAddr) {
		return fmt.Errorf("invalid aftermarket contract address: %s", m.cfg.AftermarketContractAddr)
	}

	if !common.IsHexAddress(m.cfg.VehicleContractAddr) {
		return fmt.Errorf("invalid vehicle contract address: %s", m.cfg.VehicleContractAddr)
	}

	if m.cfg.ChainID == 0 {
		return fmt.Errorf("chain_id not set")
	}

	return nil
}

// SignalConvert converts a message to signals.
func (m *Module) SignalConvert(_ context.Context, msgBytes []byte) ([]vss.Signal, error) {
	event := cloudevent.CloudEventHeader{}
	err := json.Unmarshal(msgBytes, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}
	if event.Type != cloudevent.TypeStatus || event.Producer == event.Subject {
		return nil, nil
	}
	signals, err := status.SignalsFromV2Payload(msgBytes)
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
func (m Module) CloudEventConvert(_ context.Context, msgData []byte) ([][]byte, error) {
	var result [][]byte

	var event AutopiEvent
	err := json.Unmarshal(msgData, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal record data: %w", err)
	}
	if event.DeviceTokenID == nil {
		return nil, fmt.Errorf("device token id is missing")
	}

	// handle both status and fingerprint events
	var eventType string
	if event.Type == StatusEventType {
		eventType = cloudevent.TypeStatus
	} else if event.Type == FingerprintEventType {
		eventType = cloudevent.TypeFingerprint
	} else {
		return nil, fmt.Errorf("unknown event type: %s", event.Type)
	}

	// Construct the producer DID
	producer := cloudevent.NFTDID{
		ChainID:         m.cfg.ChainID,
		ContractAddress: common.HexToAddress(m.cfg.AftermarketContractAddr),
		TokenID:         *event.DeviceTokenID,
	}.String()

	// Construct the subject
	var subject string
	if event.VehicleTokenID != nil {
		subject = cloudevent.NFTDID{
			ChainID:         m.cfg.ChainID,
			ContractAddress: common.HexToAddress(m.cfg.VehicleContractAddr),
			TokenID:         uint32(*event.VehicleTokenID),
		}.String()
	}

	cloudEvent, err := convertToCloudEvent(event, producer, subject, eventType)
	if err != nil {
		return nil, err
	}
	// Append the status event to the result
	result = append(result, cloudEvent)

	// Each AP payload has device information, so we need to create separate status event where subject == producer
	cloudEventDevice, err := convertToCloudEvent(event, producer, producer, cloudevent.TypeStatus)
	if err != nil {
		return nil, err
	}

	// Append the status event to the result
	result = append(result, cloudEventDevice)

	return result, nil
}

// convertToCloudEvent wraps an AutopiEvent into a CloudEvent.
// Returns:
//   - A byte slice containing the JSON representation of the CloudEvent.
//   - An error if the CloudEvent creation or marshaling fails.
func convertToCloudEvent(event AutopiEvent, producer, subject, eventType string) ([]byte, error) {
	cloudEvent, err := createCloudEvent(event, producer, subject, eventType)
	if err != nil {
		return nil, err
	}

	cloudEventBytes, err := json.Marshal(cloudEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cloudEvent: %w", err)
	}
	return cloudEventBytes, nil
}

// createCloudEvent creates a cloud event from autopi event.
func createCloudEvent(event AutopiEvent, producer, subject, eventType string) (cloudevent.CloudEvent[json.RawMessage], error) {
	timeValue, err := time.Parse(time.RFC3339, event.Time)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, fmt.Errorf("Failed to parse time: %v\n", err)
	}
	return cloudevent.CloudEvent[json.RawMessage]{
		CloudEventHeader: cloudevent.CloudEventHeader{
			DataContentType: "application/json",
			ID:              ksuid.New().String(),
			Subject:         subject,
			Source:          "dimo/integration/27qftVRWQYpVDcO5DltO5Ojbjxk",
			SpecVersion:     "1.0",
			Time:            timeValue,
			Type:            eventType,
			DataVersion:     DataVersion,
			Producer:        producer,
			Extras: map[string]any{
				"signature": event.Signature,
			},
		},
		Data: event.Data,
	}, nil
}
