package macaron

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/DIMO-Network/DIS/internal/service/deviceapi"
	"github.com/DIMO-Network/DIS/internal/tokengetter"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/DIMO-Network/model-garage/pkg/vss/convert"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/mod/semver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MacaronModule is a module that converts macaron messages to signals.
type MacaronModule struct {
	tokenGetter convert.TokenIDGetter
	logger      *service.Logger
}

// SetLogger sets the logger for the module.
func (m *MacaronModule) SetLogger(logger *service.Logger) {
	m.logger = logger
}

// SetConfig sets the configuration for the module.
func (m *MacaronModule) SetConfig(config []byte) error {
	devicesAPIGRPCAddr := string(config)
	var deviceAPI convert.TokenIDGetter
	if devicesAPIGRPCAddr == "test" {
		deviceAPI = &testGetter{}
	} else {
		devicesConn, err := grpc.NewClient(devicesAPIGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to dial devices api: %w", err)
		}
		deviceAPI = deviceapi.NewService(devicesConn)
	}
	limitedDeviceAPI := tokengetter.NewLimitedTokenGetter(deviceAPI, m.logger)
	m.tokenGetter = limitedDeviceAPI
	return nil
}

// SignalConvert converts a message to signals.
func (m MacaronModule) SignalConvert(ctx context.Context, msgBytes []byte) ([]vss.Signal, error) {
	schemaVersion := convert.GetSchemaVersion(msgBytes)
	if semver.Compare(convert.StatusV1Converted, schemaVersion) == 0 {
		// ignore v1.1 messages
		return nil, nil
	}
	signals, err := convert.SignalsFromPayload(ctx, m.tokenGetter, msgBytes)
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

type testGetter struct{}

func (*testGetter) TokenIDFromSubject(_ context.Context, subject string) (uint32, error) {
	if subject == notFoundSubject {
		return 0, fmt.Errorf("%w: no tokenID set", deviceapi.NotFoundError{DeviceID: subject})
	}
	if subject == errorSubject {
		return 0, errors.New("test error")
	}
	id, err := strconv.Atoi(subject)
	return uint32(id), err
}

const (
	notFoundSubject = "not_found"
	errorSubject    = "error"
)
