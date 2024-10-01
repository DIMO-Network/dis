package ruptela

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	dimoshared "github.com/DIMO-Network/shared"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var chainID string
var aftermarketContractAddr string
var vehicleContractAddr string

// RuptelaModule is a module that converts ruptela messages to signals.
type RuptelaModule struct {
	TokenGetter convert.TokenIDGetter
	logger      *service.Logger
}

// New creates a new RuptelaModule.
func New() (*RuptelaModule, error) {
	aftermarketContractAddr = os.Getenv("AFTERMARKET_CONTRACT_ADDR")
	vehicleContractAddr = os.Getenv("VEHICLE_CONTRACT_ADDR")
	chainID = os.Getenv("CHAIN_ID")
	if aftermarketContractAddr == "" || vehicleContractAddr == "" || chainID == "" {
		return nil, errors.New("missing aftermarket or vehicle contract address or chain ID")
	}
	return &RuptelaModule{}, nil
}

// SetLogger sets the logger for the module.
func (m *RuptelaModule) SetLogger(logger *service.Logger) {
	m.logger = logger
}

// SetConfig sets the configuration for the module.
func (m *RuptelaModule) SetConfig(config string) error {
	return nil
}

// SignalConvert converts a message to signals.
func (m RuptelaModule) SignalConvert(ctx context.Context, msgBytes []byte) ([]vss.Signal, error) {
	return nil, errors.New("ruptela signal conversion not implemented")
}

// CloudEventConvert converts a message to cloud events.
func (RuptelaModule) CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error) {
	var result [][]byte

	var event RuptelaEvent
	err := json.Unmarshal(msgData, &event)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal record data: %v\n", err)
	}

	// Construct the producer DID
	producer := constructDID(aftermarketContractAddr, event.TokenID)
	subject, err := determineSubject(event, producer)
	if err != nil {
		return nil, err
	}

	cloudEvent, err := createCloudEvent(event, producer, subject, "status")
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
	if event.DS == "r/v0/s" {
		additionalEvents, err := handleStatusEvent(event, producer, subject)
		if err != nil {
			return nil, err
		}
		result = append(result, additionalEvents...)
	}

	return result, nil
}

// determineSubject determines the subject of the cloud event based on the DS type.
func determineSubject(event RuptelaEvent, producer string) (string, error) {
	var subject string
	switch event.DS {
	case "r/v0/s", "r/v0/loc":
		subject = constructDID(vehicleContractAddr, event.VehicleTokenID)
	case "r/v0/dev":
		subject = producer
	default:
		return "", fmt.Errorf("unknown DS type: %s", event.DS)
	}
	return subject, nil
}

// handleStatusEvent add fingerprint event to the result if the VIN is present in the payload.
func handleStatusEvent(event RuptelaEvent, producer, subject string) ([][]byte, error) {
	isVinPresent, err := checkVinPresenceInPayload(event.Data)
	if err != nil {
		return nil, err
	}

	if !isVinPresent {
		return nil, nil
	}

	cloudEventFingerprint, err := createCloudEvent(event, producer, subject, "fingerprint")
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
func createCloudEvent(event RuptelaEvent, producer, subject, eventType string) (CloudEvent[json.RawMessage], error) {
	timeValue, err := time.Parse(time.RFC3339, event.Time)
	if err != nil {
		return CloudEvent[json.RawMessage]{}, fmt.Errorf("Failed to parse time: %v\n", err)
	}
	return CloudEvent[json.RawMessage]{
		CloudEvent: dimoshared.CloudEvent[json.RawMessage]{
			Data:            event.Data,
			DataContentType: "application/json",
			ID:              uuid.New().String(),
			Subject:         subject,
			Source:          "dimo/integration/2lcaMFuCO0HJIUfdq8o780Kx5n3",
			SpecVersion:     "1.0",
			Time:            timeValue,
			Type:            eventType,
		},
		DataVersion: "1.0.0",
		Producer:    producer,
		Signature:   event.Signature,
	}, nil
}

// constructDID constructs a DID from the chain ID, contract address, and token ID.
func constructDID(contractAddress string, tokenID uint64) string {
	return fmt.Sprintf("did:nft:%s:%s_%s", chainID, contractAddress, strconv.FormatUint(tokenID, 10))
}

// checkVinPresenceInPayload checks if the VIN is present in the payload.
func checkVinPresenceInPayload(eventData json.RawMessage) (bool, error) {
	var dataContent DataContent
	err := json.Unmarshal(eventData, &dataContent)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal data: %v\n", err)
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

func marshalCloudEvent(cloudEvent CloudEvent[json.RawMessage]) ([]byte, error) {
	cloudEventBytes, err := json.Marshal(cloudEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cloudEvent: %v", err)
	}
	return cloudEventBytes, nil
}
