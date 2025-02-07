package compass

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/compass/status"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/gjson"
	"regexp"
	"time"
)

type moduleConfig struct {
	ChainID             uint64 `json:"chain_id"`
	SynthContractAddr   string `json:"synth_contract_addr"`
	VehicleContractAddr string `json:"vehicle_contract_addr"`
}

// Module is a module that converts compass messages to signals.
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

	if !common.IsHexAddress(m.cfg.SynthContractAddr) {
		return fmt.Errorf("invalid aftermarket contract address: %s", m.cfg.SynthContractAddr)
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
	signals, err := status.Decode(msgBytes)
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
func (m *Module) CloudEventConvert(_ context.Context, msgData []byte) ([]cloudevent.CloudEventHeader, []byte, error) {
	var event CompassEvent
	err := json.Unmarshal(msgData, &event)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal record data: %w", err)
	}
	if event.DeviceTokenID == nil {
		return nil, nil, fmt.Errorf("device token id is missing")
	}

	// Construct the producer DID
	producer := cloudevent.NFTDID{
		ChainID:         m.cfg.ChainID,
		ContractAddress: common.HexToAddress(m.cfg.SynthContractAddr),
		TokenID:         *event.DeviceTokenID,
	}.String()

	// Construct the subject
	subject := cloudevent.NFTDID{
		ChainID:         m.cfg.ChainID,
		ContractAddress: common.HexToAddress(m.cfg.VehicleContractAddr),
		TokenID:         *event.VehicleTokenID,
	}.String()

	statusHdr, err := createCloudEventHdr(&event, producer, subject, cloudevent.TypeStatus)
	if err != nil {
		return nil, nil, err
	}
	eventHeaders := []cloudevent.CloudEventHeader{statusHdr}

	isVinPresent, err := checkVINPresenceInPayload(&event)
	if err != nil {
		return nil, nil, err
	}

	if isVinPresent {
		fpHdr, errCE := createCloudEventHdr(&event, producer, subject, cloudevent.TypeFingerprint)
		if errCE != nil {
			return nil, nil, errCE
		}
		eventHeaders = append(eventHeaders, fpHdr)
	}

	return eventHeaders, event.Data, nil
}

// checkVINPresenceInPayload checks if the VIN is present in the payload.
func checkVINPresenceInPayload(event *CompassEvent) (bool, error) {
	// Extract the vehicle_id field using gjson
	vehicleID := gjson.GetBytes(event.Data, "vehicle_id").String()
	if vehicleID == "" || !isValidVIN(vehicleID) {
		return false, nil
	}

	return true, nil
}

func isValidVIN(vin string) bool {
	// Define a regex pattern for a valid VIN (17 characters, alphanumeric)
	vinPattern := `^[A-HJ-NPR-Z0-9]{17}$`
	vinRegex := regexp.MustCompile(vinPattern)
	return vinRegex.MatchString(vin)
}

// createCloudEvent creates a cloud event from a compass event.
func createCloudEventHdr(event *CompassEvent, producer, subject, eventType string) (cloudevent.CloudEventHeader, error) {
	timeValue, err := time.Parse(time.RFC3339, event.Time)
	if err != nil {
		return cloudevent.CloudEventHeader{}, fmt.Errorf("Failed to parse time: %v\n", err)
	}
	return cloudevent.CloudEventHeader{
		DataContentType: "application/json",
		ID:              ksuid.New().String(),
		Subject:         subject,
		SpecVersion:     "1.0",
		Time:            timeValue,
		Type:            eventType,
		DataVersion:     "1.0",
		Producer:        producer,
	}, nil
}
