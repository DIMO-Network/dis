package signalconvert

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName           = "dimo_signal_convert"
	vehicleAddressFieldName = "vehicle_nft_address"
	chainIDFieldName        = "chain_id"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts events into a list of signals").
	Field(service.NewIntField(chainIDFieldName).Description("Chain Id for the Ethereum network")).
	Field(service.NewStringField(vehicleAddressFieldName).Description("Ethereum address for the vehicles contract"))

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	chainID, err := cfg.FieldInt(chainIDFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", chainIDFieldName, err)
	}
	vehicleAddress, err := cfg.FieldString(vehicleAddressFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", vehicleAddressFieldName, err)
	}
	if !common.IsHexAddress(vehicleAddress) {
		return nil, fmt.Errorf("invalid vehicle contract address: %s", vehicleAddress)
	}
	met := mgr.Metrics()
	met.NewCounter("signal_convert_errors", "Errors encountered during signal conversion")
	return &vssProcessor{
		logger:            mgr.Logger(),
		vehicleNFTAddress: common.HexToAddress(vehicleAddress),
		chainID:           uint64(chainID),
	}, nil
}
