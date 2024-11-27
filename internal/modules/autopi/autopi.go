package autopi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DIMO-Network/model-garage/pkg/autopi"
	"github.com/DIMO-Network/model-garage/pkg/autopi/status"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
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
	return autopi.ConvertToCloudEvents(msgData, m.cfg.ChainID, m.cfg.AftermarketContractAddr, m.cfg.VehicleContractAddr)
}
