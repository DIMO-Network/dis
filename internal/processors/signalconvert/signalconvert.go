package signalconvert

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
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

	subjectDID, err := cloudevent.DecodeERC721DID(rawEvent.Subject)
	if err != nil {
		// fail this message if we unexpectedly can't decode the subject DID
		msg.SetError(fmt.Errorf("failed to decode subject DID during signal convert which expects valid cloudevents: %w", err))
		return retBatch
	}
	signals, err := modules.ConvertToSignals(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		errMsg := msg.Copy()
		var convertErr *convert.ConversionError
		if errors.As(err, &convertErr) {
			// if this is a conversion error, we can use the decoded signals and the errors
			err = errors.Join(convertErr.Errors...)
			signals = convertErr.DecodedSignals
		}
		processors.SetError(errMsg, processorName, "error converting signals", err)
		retBatch = append(retBatch, errMsg)
	}

	if len(signals) == 0 {
		return retBatch
	}

	signals, futureDupeErr := pruneFutureAndDuplicateSignals(signals)
	signals, locationErr := handleCoordinates(signals)

	if comboErr := errors.Join(futureDupeErr, locationErr); comboErr != nil {
		errMsg := msg.Copy()
		errMsg.SetError(comboErr)
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

// pruneFutureAndDuplicateSignals removes signals that are not valid and returns an error for each invalid signal.
func pruneFutureAndDuplicateSignals(signals []vss.Signal) ([]vss.Signal, error) {
	var errs error
	slices.SortFunc(signals, func(a, b vss.Signal) int {
		return cmp.Or(a.Timestamp.Compare(b.Timestamp), cmp.Compare(a.Name, b.Name))
	})
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

func signalEqual(a, b vss.Signal) bool {
	return a.Name == b.Name && a.Timestamp.Equal(b.Timestamp) && a.TokenID == b.TokenID
}

func setMetaData(signal *vss.Signal, rawEvent *cloudevent.RawEvent, subject cloudevent.ERC721DID) {
	signal.Source = rawEvent.Source
	signal.Producer = rawEvent.Producer
	signal.CloudEventID = rawEvent.ID
	if subject.TokenID != nil {
		signal.TokenID = uint32(subject.TokenID.Uint64())
	}
}

func (v *vssProcessor) isVehicleSignalMessage(rawEvent *cloudevent.RawEvent) bool {
	if rawEvent.Type != cloudevent.TypeStatus {
		return false
	}

	did, err := cloudevent.DecodeERC721DID(rawEvent.Subject)
	if err != nil {
		return false
	}
	if did.ChainID != v.chainID || did.ContractAddress.Cmp(v.vehicleNFTAddress) != 0 {
		return false
	}
	return true
}
