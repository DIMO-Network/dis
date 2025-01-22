package signalconvert

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

var (
	errLatLongMismatch = errors.New("latitude and longitude mismatch")
	errFutureTimestamp = errors.New("future timestamp")
	pruneSignal        = vss.Signal{Name: pruneSignalName}
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
			msgCpy := msg.Copy()
			msgCpy.SetStructured(signals[i])
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
	var errs error
	slices.SortFunc(signals, func(a, b vss.Signal) int {
		if a.Timestamp.Before(b.Timestamp) {
			return -1
		}
		if a.Timestamp.After(b.Timestamp) {
			return 1
		}
		return cmp.Compare(a.Name, b.Name)
	})
	lastLat := -1
	for i, signal := range signals {
		if processors.IsFutureTimestamp(signal.Timestamp) {
			errs = errors.Join(errs, fmt.Errorf("%w, signal '%s' has timestamp: %v", errFutureTimestamp, signal.Name, signal.Timestamp))
			signals[i] = pruneSignal
			continue
		}
		if signal.Name == vss.FieldCurrentLocationLatitude {
			if lastLat != -1 {
				// We hit another latitude signal before a longitude signal
				// prune the previous latitude signal it doesn't have a matching longitude signal
				prevLat := signals[lastLat]
				errs = errors.Join(errs, fmt.Errorf("%w, latitude at time %v is misssing matching longitude", errLatLongMismatch, prevLat.Timestamp))
				signals[lastLat] = pruneSignal
			}
			lastLat = i
		} else if signal.Name == vss.FieldCurrentLocationLongitude {
			if lastLat == -1 {
				// We hit a longitude signal before a latitude signal
				// prune this longitude signal it doesn't have a matching latitude signal
				errs = errors.Join(errs, fmt.Errorf("%w, longitude at time %v is misssing matching latitude", errLatLongMismatch, signal.Timestamp))
				signals[i] = pruneSignal
			} else {
				// check that this longitude signals is within half a second of the last latitude signal
				lat := signals[lastLat]
				dur := signal.Timestamp.Sub(lat.Timestamp)
				if dur > time.Second/2 {
					// prune the latitude signal and this longitude signal they are too far apart
					errs = errors.Join(errs, fmt.Errorf("%w, latitude at time %v is misssing matching longitude", errLatLongMismatch, lat.Timestamp))
					errs = errors.Join(errs, fmt.Errorf("%w, longitude at time %v is misssing matching latitude", errLatLongMismatch, signal.Timestamp))
					signals[i] = pruneSignal
					signals[lastLat] = pruneSignal
				}
				lastLat = -1
			}
		}
	}
	// after the last signal was checked if we still have a latitude signal without a matching longitude signal prune it
	if lastLat != -1 {
		prevLat := signals[lastLat]
		errs = errors.Join(errs, fmt.Errorf("%w, latitude at time %v is misssing matching longitude", errLatLongMismatch, prevLat.Timestamp))
		signals[lastLat] = pruneSignal
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
