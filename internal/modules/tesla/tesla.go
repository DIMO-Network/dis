package tesla

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/tesla/status"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// TODO(elffjs): Not sure what to actually do with these.
type moduleConfig struct {
	ChainID               uint64 `json:"chain_id"`
	SyntheticContractAddr string `json:"synthetic_contract_addr"`
	VehicleContractAddr   string `json:"vehicle_contract_addr"`
}

type Module struct {
	logger *service.Logger
	cfg    moduleConfig
}

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

	if !common.IsHexAddress(m.cfg.SyntheticContractAddr) {
		return fmt.Errorf("invalid synthetic contract address: %s", m.cfg.SyntheticContractAddr)
	}

	if !common.IsHexAddress(m.cfg.VehicleContractAddr) {
		return fmt.Errorf("invalid vehicle contract address: %s", m.cfg.VehicleContractAddr)
	}

	if m.cfg.ChainID == 0 {
		return fmt.Errorf("chain_id not set")
	}

	return nil
}

func (m *Module) SignalConvert(_ context.Context, msgBytes []byte) ([]vss.Signal, error) {
	return status.Decode(msgBytes)
}

// CloudEventConvert converts a message to cloud events.
func (m Module) CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error) {
	return [][]byte{msgData}, nil
}
