package signalconvert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/dis/internal/modules"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	signalValidContentType = "dimo_valid_signal"
	pruneSignalName        = "___prune"
)

type SignalModule interface {
	SignalConvert(ctx context.Context, msgData []byte) ([]vss.Signal, error)
}
type vssProcessor struct {
	signalModule SignalModule
	Logger       *service.Logger
}

// Close to fulfill the service.Processor interface.
func (*vssProcessor) Close(context.Context) error {
	return nil
}

func newVSSProcessor(lgr *service.Logger, moduleName, moduleConfig string) (*vssProcessor, error) {
	decodedModuelConfig, err := base64.StdEncoding.DecodeString(moduleConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decode module config: %w", err)
	}
	moduleOpts := modules.Options{
		Logger:       lgr,
		FilePath:     "",
		ModuleConfig: string(decodedModuelConfig),
	}
	signalModule, err := modules.LoadSignalModule(moduleName, moduleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to load signal module: %w", err)
	}
	return &vssProcessor{
		signalModule: signalModule,
		Logger:       lgr,
	}, nil
}

func (v *vssProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		// keep the original message and add any new signal messages to the batch
		retBatch := service.MessageBatch{msg}
		errMsg := msg.Copy()
		msgBytes, err := msg.AsBytes()
		if err != nil {
			// Add the error to the batch and continue to the next message.
			errMsg.SetError(fmt.Errorf("failed to get msg bytes: %w", err))
			retBatches = append(retBatches, service.MessageBatch{errMsg})
			continue
		}
		signals, err := v.signalModule.SignalConvert(ctx, msgBytes)
		if err != nil {
			errMsg.SetError(err)
			data, err := json.Marshal(err)
			if err == nil {
				errMsg.SetBytes(data)
			}
			retBatch = append(retBatch, errMsg)
		}
		signals, err = pruneSignals(signals)
		if err != nil {
			errMsg.SetError(err)
			retBatch = append(retBatch, errMsg)
		}

		for i := range signals {
			sigVals := vss.SignalToSlice(signals[i])
			msgCpy := msg.Copy()
			msgCpy.SetStructured(sigVals)
			msgCpy.MetaSetMut(processors.MessageContentKey, signalValidContentType)
			retBatch = append(retBatch, msgCpy)
		}
		retBatches = append(retBatches, retBatch)
	}
	return retBatches, nil
}

type LatLngIdx struct {
	Latitude  *int `json:"latitude"`
	Longitude *int `json:"longitude"`
}

// pruneSignals removes signals that are not valid and returns an error for each invalid signal.
func pruneSignals(signals []vss.Signal) ([]vss.Signal, error) {
	now := time.Now()
	var errs error
	pruneSignal := vss.Signal{Name: pruneSignalName}
	latLongPairs := map[int64]LatLngIdx{}
	for i, signal := range signals {
		if signal.Timestamp.After(now) {
			errors.Join(errs, fmt.Errorf("signal %s has a future timestamp %s", signal.Name, signal.Timestamp))
			signals[i] = pruneSignal
			continue
		}
		timeInMilli := signal.Timestamp.UnixMilli()
		if signal.Name == vss.FieldCurrentLocationLatitude {
			latLng := latLongPairs[timeInMilli]
			latLng.Latitude = &i
			latLongPairs[timeInMilli] = latLng
		} else if signal.Name == vss.FieldCurrentLocationLongitude {
			latLng := latLongPairs[timeInMilli]
			latLng.Longitude = &i
			latLongPairs[timeInMilli] = latLng
		}
	}
	for _, latLng := range latLongPairs {
		// check if one of the lat or long is missing
		if latLng.Latitude == nil && latLng.Longitude != nil {
			errors.Join(errs, fmt.Errorf("longitude at time %v is misssing matching latitude", signals[*latLng.Longitude].Timestamp))
			signals[*latLng.Longitude] = pruneSignal
		}
		if latLng.Latitude != nil && latLng.Longitude == nil {
			errors.Join(errs, fmt.Errorf("latitude at time %v is misssing matching longitude", signals[*latLng.Latitude].Timestamp))
			signals[*latLng.Latitude] = pruneSignal
		}
	}
	// remove all the pruned signals
	var prunedSignals []vss.Signal
	for _, signal := range signals {
		if signal.Name != pruneSignalName {
			prunedSignals = append(prunedSignals, signal)
		}
	}
	return prunedSignals, errs
}
