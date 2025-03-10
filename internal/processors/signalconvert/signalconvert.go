package signalconvert

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	signalValidContentType = "dimo_valid_signal"
	pruneSignalName        = "___prune"
	maxLatLongDur          = time.Second / 2
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
	Logger *service.Logger
}

// Close to fulfill the service.Processor interface.
func (*vssProcessor) Close(context.Context) error {
	return nil
}

func (v *vssProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		// keep the original message and add any new signal messages to the batch
		retBatch := service.MessageBatch{msg}
		errMsg := msg.Copy()
		msgStruct, err := msg.AsStructured()
		if err != nil {
			// Add the error to the batch and continue to the next message.
			errMsg.SetError(fmt.Errorf("failed to get msg struct: %w", err))
			retBatches = append(retBatches, service.MessageBatch{errMsg})
			continue
		}
		rawEvent, ok := msgStruct.(*cloudevent.RawEvent)
		if !ok {
			errMsg.SetError(errors.New("failed to cast to cloudevent.RawEvent"))
			retBatches = append(retBatches, service.MessageBatch{errMsg})
			continue
		}
		signals, err := modules.ConvertToSignals(ctx, rawEvent.Source, *rawEvent)
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
			v.setMetaData(&signals[i], rawEvent)
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
	lastCord := -1
	for i := range signals {
		signal := &signals[i]

		// prune future signals
		if processors.IsFutureTimestamp(signal.Timestamp) {
			errs = errors.Join(errs, fmt.Errorf("%w, signal '%s' has timestamp: %v", errFutureTimestamp, signal.Name, signal.Timestamp))
			signals[i] = pruneSignal
			continue
		}

		// prune duplicate signals
		if i < len(signals)-1 {
			if signalEqual(signals[i], signals[i+1]) {
				signals[i] = pruneSignal
				continue
			}
		}

		// prune latitude and longitude signals that don't have a matching signal
		lastCord, errs = pruneLatLngSignals(&signals, lastCord, i, errs)
	}
	// after the last signal was checked if we still have a latitude signal without a matching longitude signal prune it
	if lastCord != -1 {
		prevCord := signals[lastCord]
		errs = errors.Join(errs, fmt.Errorf("%w, signal '%s' at time %v is missing matching coordinate", errLatLongMismatch, prevCord.Name, prevCord.Timestamp))
		signals[lastCord] = pruneSignal
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

// pruneLatLngSignals checks if the current signal is a latitude or longitude signal and prunes the previous signal if it isn't a pair.
// this logic is separated in a function for easier control flow.
func pruneLatLngSignals(signals *[]vss.Signal, lastCord, currIdx int, errs error) (int, error) {
	if signals == nil {
		return lastCord, errs
	}

	// if the current signal is not a latitude or longitude signal return with no changes
	currCord := (*signals)[currIdx]
	if currCord.Name != vss.FieldCurrentLocationLatitude && currCord.Name != vss.FieldCurrentLocationLongitude {
		return lastCord, errs
	}

	// if we don't have a previous coordinate signal return the current index
	if lastCord == -1 {
		return currIdx, errs
	}
	prevCord := (*signals)[lastCord]

	// if we have two lats or two longs prune the previous one
	if prevCord.Name == currCord.Name {
		retErr := errors.Join(errs, fmt.Errorf("%w, signal '%s' at time %v is missing matching coordinate", errLatLongMismatch, prevCord.Name, prevCord.Timestamp))
		(*signals)[lastCord] = pruneSignal
		return currIdx, retErr
	}

	// if the two signals are too far apart prune the previous signal it doesn't have a matching signal
	dur := currCord.Timestamp.Sub(prevCord.Timestamp)
	if dur > maxLatLongDur {
		retErr := errors.Join(errs, fmt.Errorf("%w, signal '%s' at time %v is missing matching coordinate within %v", errLatLongMismatch, prevCord.Name, prevCord.Timestamp, maxLatLongDur))
		(*signals)[lastCord] = pruneSignal
		return currIdx, retErr
	}

	// if the two signals are within half a second of each other keep both and reset the lastCord
	return -1, errs
}

func signalEqual(a, b vss.Signal) bool {
	return a.Name == b.Name && a.Timestamp.Equal(b.Timestamp) && a.TokenID == b.TokenID
}

func (v *vssProcessor) setMetaData(signal *vss.Signal, rawEvent *cloudevent.RawEvent) {
	signal.Source = rawEvent.Source
	signal.Producer = rawEvent.Producer
	signal.CloudEventID = rawEvent.ID
	subjectDID, err := cloudevent.DecodeNFTDID(rawEvent.Subject)
	if err != nil {
		v.Logger.Warnf("failed to decode subject DID during signal convert which expects valid cloudevents: %v", err)
	} else {
		signal.TokenID = subjectDID.TokenID
	}
}
