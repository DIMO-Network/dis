package macaron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DIMO-Network/DIS/internal/service/deviceapi"
	"github.com/DIMO-Network/DIS/internal/tokengetter"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/mod/semver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type moduleConfig struct {
	DevicesAPIGRPCAddr string `json:"devices_api_grpc_addr"`
}

// MacaronModule is a module that converts macaron messages to signals.
type MacaronModule struct {
	TokenGetter convert.TokenIDGetter
	logger      *service.Logger
}

// New creates a new MacaronModule.
func New() (*MacaronModule, error) {
	return &MacaronModule{}, nil
}

// SetLogger sets the logger for the module.
func (m *MacaronModule) SetLogger(logger *service.Logger) {
	m.logger = logger
}

// SetConfig sets the configuration for the module.
func (m *MacaronModule) SetConfig(config string) error {
	var cfg moduleConfig
	err := json.Unmarshal([]byte(config), &cfg)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	devicesConn, err := grpc.NewClient(cfg.DevicesAPIGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial devices api: %w", err)
	}

	deviceAPI := deviceapi.NewService(devicesConn)
	limitedDeviceAPI := tokengetter.NewLimitedTokenGetter(deviceAPI, m.logger)
	m.TokenGetter = limitedDeviceAPI
	return nil
}

// SignalConvert converts a message to signals.
func (m MacaronModule) SignalConvert(ctx context.Context, msgBytes []byte) ([]vss.Signal, error) {
	schemaVersion := convert.GetSchemaVersion(msgBytes)
	if semver.Compare(convert.StatusV1Converted, schemaVersion) == 0 {
		// ignore v1.1 messages
		return nil, nil
	}
	signals, err := convert.SignalsFromPayload(ctx, m.TokenGetter, msgBytes)
	if err == nil {
		return signals, nil
	}

	if errors.As(err, &deviceapi.NotFoundError{}) {
		// If we do not have an Token for this device we want to drop the message. But we don't want to log an error.
		m.logger.Trace(fmt.Sprintf("dropping message: %v", err))
		return nil, nil
	}

	convertErr := convert.ConversionError{}
	if !errors.As(err, &convertErr) {
		// Add the error to the batch and continue to the next message.
		return nil, fmt.Errorf("failed to convert signals: %w", err)
	}

	return convertErr.DecodedSignals, convertErr
}

// CloudEventConvert converts a macaron message to cloud events.
func (MacaronModule) CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error) {
	return [][]byte{msgData}, nil
}
