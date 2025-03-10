package cloudeventconvert

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName               = "dimo_cloudevent_convert"
	didValuesFieldName          = "did_values"
	vehicleAddressFieldName     = "vehicle_nft_address"
	aftermarketAddressFieldName = "aftermarket_nft_address"
	syntheticAddressFieldName   = "synthetic_nft_address"
	chainIDFieldName            = "chain_id"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts raw payloads into cloudevents").
	Field(service.NewIntField(chainIDFieldName).Description("Chain Id for the Ethereum network")).
	Field(service.NewStringField(vehicleAddressFieldName).Description("Ethereum address for the vehicles contract")).
	Field(service.NewStringField(aftermarketAddressFieldName).Description("Ethereum address for the aftermarket contract")).
	Field(service.NewStringField(syntheticAddressFieldName).Description("Ethereum address for the synthetic device contract"))

func init() {
	err := service.RegisterBatchProcessor(processorName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	vehicleAddress, err := cfg.FieldString(vehicleAddressFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", vehicleAddressFieldName, err)
	}
	if !common.IsHexAddress(vehicleAddress) {
		return nil, fmt.Errorf("invalid vehicle contract address: %s", vehicleAddress)
	}
	aftermarketAddress, err := cfg.FieldString(aftermarketAddressFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", aftermarketAddressFieldName, err)
	}
	if !common.IsHexAddress(aftermarketAddress) {
		return nil, fmt.Errorf("invalid aftermarket contract address: %s", aftermarketAddress)
	}
	syntheticAddress, err := cfg.FieldString(syntheticAddressFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", syntheticAddressFieldName, err)
	}
	if !common.IsHexAddress(syntheticAddress) {
		return nil, fmt.Errorf("invalid synthetic contract address: %s", syntheticAddress)
	}
	chainID, err := cfg.FieldInt(chainIDFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", chainIDFieldName, err)
	}

	return newCloudConvertProcessor(mgr.Logger(), uint64(chainID),
		common.HexToAddress(vehicleAddress),
		common.HexToAddress(aftermarketAddress),
		common.HexToAddress(syntheticAddress)), nil
}
