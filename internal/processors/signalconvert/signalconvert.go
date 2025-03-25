package signalconvert

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
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

type vssProcessor struct {
	logger            *service.Logger
	vehicleNFTAddress common.Address
	chainID           uint64
}

// Close to fulfill the service.Processor interface.
func (*vssProcessor) Close(context.Context) error {
	return nil
}

// ProcessBatch to fulfill the service.BatchProcessor interface.
func (v *vssProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		retBatches = append(retBatches, v.processMsg(ctx, msg))
	}
	return retBatches, nil
}

// processMsg processes a single message and returns a batch of signals and or errors.
func (v *vssProcessor) processMsg(ctx context.Context, msg *service.Message) service.MessageBatch {
	// keep the original message and add any new signal messages to the batch
	retBatch := service.MessageBatch{msg}
	rawEvent, err := processors.MsgToEvent(msg)
	if err != nil || !v.isVehicleSignalMessage(rawEvent) {
		// leave the message as is and continue to the next message
		return retBatch
	}
	subjectDID, err := cloudevent.DecodeNFTDID(rawEvent.Subject)
	if err != nil {
		// fail this message if we unexpectedly can't decode the subject DID
		msg.SetError(fmt.Errorf("failed to decode subject DID during signal convert which expects valid cloudevents: %w", err))
		return retBatch
	}
	signals, partialErr := modules.ConvertToSignals(ctx, rawEvent.Source, *rawEvent)
	if partialErr != nil {
		errMsg := msg.Copy()
		errMsg.SetError(partialErr)
		data, err := json.Marshal(partialErr)
		if err == nil {
			errMsg.SetBytes(data)
		}
		retBatch = append(retBatch, errMsg)
	}
	signals, partialErr = pruneSignals(signals)
	if partialErr != nil {
		errMsg := msg.Copy()
		errMsg.SetError(partialErr)
		retBatch = append(retBatch, errMsg)
	}

	for i := range signals {
		msgCpy := msg.Copy()
		setMetaData(&signals[i], rawEvent, subjectDID)
		msgCpy.SetStructured(signals[i])
		msgCpy.MetaSetMut(processors.MessageContentKey, signalValidContentType)
		retBatch = append(retBatch, msgCpy)
	}
	return retBatch
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

func setMetaData(signal *vss.Signal, rawEvent *cloudevent.RawEvent, subject cloudevent.NFTDID) {
	signal.Source = rawEvent.Source
	signal.Producer = rawEvent.Producer
	signal.CloudEventID = rawEvent.ID
	signal.TokenID = subject.TokenID
}

func (v *vssProcessor) isVehicleSignalMessage(rawEvent *cloudevent.RawEvent) bool {
	if rawEvent.Type != cloudevent.TypeStatus {
		return false
	}
	did, err := cloudevent.DecodeNFTDID(rawEvent.Subject)
	if err != nil {
		return false
	}
	if did.ChainID != v.chainID || did.ContractAddress.Cmp(v.vehicleNFTAddress) != 0 {
		return false
	}
	return true
}
