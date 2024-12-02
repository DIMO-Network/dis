package ruptela

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/ruptela/status"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/ksuid"
)

const (
	StatusEventDS   = "r/v0/s"
	DevStatusDS     = "r/v0/dev"
	LocationEventDS = "r/v0/loc"
)

type moduleConfig struct {
	ChainID                 uint64 `json:"chain_id"`
	AftermarketContractAddr string `json:"aftermarket_contract_addr"`
	VehicleContractAddr     string `json:"vehicle_contract_addr"`
}

// Module is a module that converts ruptela messages to signals.
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
	if event.DataVersion == DevStatusDS || event.Type != cloudevent.TypeStatus {
		return nil, nil
	}
	signals, err := status.DecodeStatusSignals(msgBytes)
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
func (m Module) CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error) {
	var result [][]byte

	var event RuptelaEvent
	err := json.Unmarshal(msgData, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal record data: %w", err)
	}
	if event.DeviceTokenID == nil {
		return nil, fmt.Errorf("device token id is missing")
	}

	// Construct the producer DID
	producer := cloudevent.NFTDID{
		ChainID:         m.cfg.ChainID,
		ContractAddress: common.HexToAddress(m.cfg.AftermarketContractAddr),
		TokenID:         uint32(*event.DeviceTokenID),
	}.String()
	subject, err := m.determineSubject(event, producer)
	if err != nil {
		return nil, err
	}

	cloudEvent, err := createCloudEvent(event, producer, subject, cloudevent.TypeStatus)
	if err != nil {
		return nil, err
	}

	cloudEventBytes, err := marshalCloudEvent(cloudEvent)
	if err != nil {
		return nil, err
	}

	// Append the status event to the result
	result = append(result, cloudEventBytes)

	// If the VIN is present in the payload, create a fingerprint event
	if event.DS == StatusEventDS {
		additionalEvents, err := createAdditionalEvents(event, producer, subject)
		if err != nil {
			return nil, fmt.Errorf("failed making the additional events: %w", err)
		}
		result = append(result, additionalEvents...)
	}

	return result, nil
}

// determineSubject determines the subject of the cloud event based on the DS type.
func (m Module) determineSubject(event RuptelaEvent, producer string) (string, error) {
	var subject string
	switch event.DS {
	case StatusEventDS, LocationEventDS:
		if event.VehicleTokenID != nil {
			subject = cloudevent.NFTDID{
				ChainID:         m.cfg.ChainID,
				ContractAddress: common.HexToAddress(m.cfg.VehicleContractAddr),
				TokenID:         uint32(*event.VehicleTokenID),
			}.String()
		}
	case DevStatusDS:
		subject = producer
	default:
		return "", fmt.Errorf("unknown DS type: %s", event.DS)
	}
	return subject, nil
}

// createAdditionalEvents add fingerprint event to the result if the VIN is present in the payload.
func createAdditionalEvents(event RuptelaEvent, producer, subject string) ([][]byte, error) {
	isVinPresent, err := checkVINPresenceInPayload(event.Data)
	if err != nil {
		return nil, err
	}

	if !isVinPresent {
		return nil, nil
	}

	cloudEventFingerprint, err := createCloudEvent(event, producer, subject, cloudevent.TypeFingerprint)
	if err != nil {
		return nil, err
	}

	cloudEventFingerprintBytes, err := marshalCloudEvent(cloudEventFingerprint)
	if err != nil {
		return nil, err
	}

	return [][]byte{cloudEventFingerprintBytes}, nil
}

// createCloudEvent creates a cloud event from a ruptela event.
func createCloudEvent(event RuptelaEvent, producer, subject, eventType string) (cloudevent.CloudEvent[json.RawMessage], error) {
	timeValue, err := time.Parse(time.RFC3339, event.Time)
	if err != nil {
		return cloudevent.CloudEvent[json.RawMessage]{}, fmt.Errorf("Failed to parse time: %v\n", err)
	}
	return cloudevent.CloudEvent[json.RawMessage]{
		CloudEventHeader: cloudevent.CloudEventHeader{
			DataContentType: "application/json",
			ID:              ksuid.New().String(),
			Subject:         subject,
			Source:          "dimo/integration/2lcaMFuCO0HJIUfdq8o780Kx5n3",
			SpecVersion:     "1.0",
			Time:            timeValue,
			Type:            eventType,
			DataVersion:     event.DS,
			Producer:        producer,
			Extras: map[string]any{
				"signature": event.Signature,
			},
		},
		Data: event.Data,
	}, nil
}

// checkVINPresenceInPayload checks if the VIN is present in the payload.
func checkVINPresenceInPayload(eventData json.RawMessage) (bool, error) {
	var dataContent DataContent
	err := json.Unmarshal(eventData, &dataContent)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal data: %w", err)
	}
	// VIN keys in the ruptela payload
	vinKeys := []string{"104", "105", "106"}

	for _, key := range vinKeys {
		value, ok := dataContent.Signals[key]
		if !ok || value == "0" {
			// key does not exist or its value is 0
			return false, nil
		}
	}
	return true, nil
}

func marshalCloudEvent(cloudEvent cloudevent.CloudEvent[json.RawMessage]) ([]byte, error) {
	cloudEventBytes, err := json.Marshal(cloudEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cloudEvent: %w", err)
	}
	return cloudEventBytes, nil
}
