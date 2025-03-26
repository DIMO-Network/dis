package cloudeventconvert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/pkg/clickhouse"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	"github.com/DIMO-Network/dis/internal/ratedlogger"
	"github.com/DIMO-Network/model-garage/pkg/autopi"
	"github.com/DIMO-Network/model-garage/pkg/compass"
	"github.com/DIMO-Network/model-garage/pkg/hashdog"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/ruptela"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/ksuid"
)

const (
	cloudEventTypeKey     = "dimo_cloudevent_type"
	cloudEventProducerKey = "dimo_cloudevent_producer"
	cloudEventSubjectKey  = "dimo_cloudevent_subject"
	cloudEventIDKey       = "dimo_cloudevent_id"
	cloudEventIndexKey    = "dimo_cloudevent_index"
	// CloudEventIndexValueKey is the key for the index value for inserting a cloud event's metadata.
	CloudEventIndexValueKey = "dimo_cloudevent_index_value"

	cloudEventValidContentType   = "dimo_valid_cloudevent"
	cloudEventPartialContentType = "dimo_partial_cloudevent"
)

type cloudeventProcessor struct {
	logger          *service.Logger
	producerLoggers map[string]*ratedlogger.Logger
}

// Close to fulfill the service.Processor interface.
func (*cloudeventProcessor) Close(context.Context) error {
	return nil
}

func newCloudConvertProcessor(lgr *service.Logger, chainID uint64, vehicleAddr, aftermarketAddr, syntheticAddr common.Address) *cloudeventProcessor {
	// AutoPi
	autoPiModule := &autopi.Module{
		AftermarketContractAddr: aftermarketAddr,
		VehicleContractAddr:     vehicleAddr,
		ChainID:                 chainID,
	}
	modules.CloudEventRegistry.Override(modules.AutoPiSource.String(), autoPiModule)

	// Ruptela
	ruptelaModule := &ruptela.Module{
		AftermarketContractAddr: aftermarketAddr,
		VehicleContractAddr:     vehicleAddr,
		ChainID:                 chainID}
	modules.CloudEventRegistry.Override(modules.RuptelaSource.String(), ruptelaModule)

	// HashDog
	hashDogModule := &hashdog.Module{
		AftermarketContractAddr: aftermarketAddr,
		VehicleContractAddr:     vehicleAddr,
		ChainID:                 chainID}
	modules.CloudEventRegistry.Override(modules.HashDogSource.String(), hashDogModule)

	// Compass IOT
	compassModule := &compass.Module{
		SynthContractAddr:   syntheticAddr,
		VehicleContractAddr: vehicleAddr,
		ChainID:             chainID}
	modules.CloudEventRegistry.Override(modules.CompassSource.String(), compassModule)

	return &cloudeventProcessor{
		logger: lgr,
	}
}

// ProcessBatch converts a batch of messages to cloud events.
func (c *cloudeventProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	retBatches := make([]service.MessageBatch, 0, len(msgs))
	for _, msg := range msgs {
		retBatches = append(retBatches, c.processMsg(ctx, msg))
	}
	return retBatches, nil
}

func (c *cloudeventProcessor) processMsg(ctx context.Context, msg *service.Message) service.MessageBatch {
	msgBytes, err := msg.AsBytes()
	if err != nil {
		processors.SetError(msg, processorName, "failed to get message as bytes", err)
		return service.MessageBatch{msg}
	}
	source, ok := msg.MetaGet(httpinputserver.DIMOCloudEventSource)
	if !ok {
		processors.SetError(msg, processorName, "failed to get source from message metadata", err)
		return service.MessageBatch{msg}
	}

	contentType, ok := msg.MetaGet(processors.MessageContentKey)
	if !ok {
		processors.SetError(msg, processorName, "failed to get content type from message metadata", err)
		return service.MessageBatch{msg}
	}

	var hdrs []cloudevent.CloudEventHeader
	var eventData []byte
	switch contentType {
	case httpinputserver.ConnectionContent:
		hdrs, eventData, err = modules.ConvertToCloudEvents(ctx, source, msgBytes)
		if err != nil {
			// Try to unmarshal convert errors
			data, marshalErr := json.Marshal(err)
			if marshalErr == nil {
				msg.SetBytes(data)
			}
			processors.SetError(msg, processorName, "failed to convert to cloud event", err)
			return service.MessageBatch{msg}
		}
		if len(hdrs) == 0 {
			processors.SetError(msg, processorName, "no cloud events headers returned", nil)
			return service.MessageBatch{msg}
		}
		if len(eventData) == 0 {
			// If the module chooses not to return data, use the original message will be used
			eventData = msgBytes
		}
	case httpinputserver.AttestationContent:
		var event cloudevent.CloudEvent[json.RawMessage]
		err = json.Unmarshal(msgBytes, &event)
		if err != nil {
			processors.SetError(msg, processorName, "failed to unmarshal attestation cloud event", err)
			return service.MessageBatch{msg}
		}

		// validRange := 5 * time.Minute
		// currentTimestamp := time.Now()
		// if !(currentTimestamp.Sub(event.Time) <= validRange && event.Time.Sub(currentTimestamp) <= validRange) {
		// 	processors.SetError(msg, processorName, fmt.Sprintf("event timestamp exceeds valid range: %+v", event.Time), nil)
		// 	return service.MessageBatch{msg}
		// }

		if event.ID == "" {
			event.ID = ksuid.New().String()
		}

		if event.DataContentType == "" {
			event.DataContentType = "application/json"
		}

		if event.SpecVersion == "" {
			event.SpecVersion = "1.0"
		}

		if event.Type == "" {
			event.Type = "dimo.attestation"
		}

		if _, err := cloudevent.DecodeNFTDID(event.Subject); err != nil {
			fmt.Println(111111)
			processors.SetError(msg, processorName, "invalid attestation subject format", err)
			return service.MessageBatch{msg}
		}

		validSignature, err := c.validateSignature(msg, event.Producer)
		if err != nil {
			processors.SetError(msg, processorName, "failed to validate signature on message", err)
			return service.MessageBatch{msg}
		}

		if !validSignature {
			processors.SetError(msg, processorName, "message signature invalid", err)
			return service.MessageBatch{msg}
		}

		hdrs = append(hdrs, event.CloudEventHeader)
	}

	var retBatch []*service.Message
	retBatch, err = c.createEventMsgs(msg, source, hdrs, eventData)
	if err != nil {
		processors.SetError(msg, processorName, "failed to create event messages", err)
		return service.MessageBatch{msg}
	}
	return retBatch
}

func (c *cloudeventProcessor) validateSignature(msg *service.Message, producer string) (bool, error) {
	producerDID, err := cloudevent.DecodeEthrDID(producer)
	if err != nil {
		return false, errors.New("failed to decode producer did")
	}

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return false, errors.New("failed to parse message as bytes")
	}

	var event cloudevent.CloudEvent[json.RawMessage]
	err = json.Unmarshal(msgBytes, &event)
	if err != nil {
		return false, fmt.Errorf("failed to parse cloud event: %w", err)
	}

	sig, ok := event.Extras["signature"].(string)
	if !ok {
		return false, errors.New("failed to get signed payload")
	}

	signature := common.FromHex(sig)
	msgHash := crypto.Keccak256(event.Data)
	pk, err := crypto.Ecrecover(msgHash, signature)
	if err != nil {
		return false, fmt.Errorf("failed to recover an recoveredPubKey: %w", err)
	}

	pubKey, err := crypto.UnmarshalPubkey(pk)
	recoveredAddress := crypto.PubkeyToAddress(*pubKey)

	return recoveredAddress == producerDID.ContractAddress, nil
}

func (c *cloudeventProcessor) createEventMsgs(origMsg *service.Message, source string, hdrs []cloudevent.CloudEventHeader, eventData []byte) ([]*service.Message, error) {
	if len(hdrs) == 0 {
		return nil, fmt.Errorf("no cloud events headers returned")
	}
	messages := make([]*service.Message, len(hdrs))
	defaultID := ksuid.New().String()
	// set defaults and metadata for each header, then create a message for each header
	for i := range hdrs {
		if processors.IsFutureTimestamp(hdrs[i].Time) {
			if c.producerLoggers == nil {
				c.producerLoggers = make(map[string]*ratedlogger.Logger)
			}
			logger, ok := c.producerLoggers[hdrs[i].Producer]
			if !ok {
				logger = ratedlogger.New(c.logger, time.Hour)
				c.producerLoggers[hdrs[i].Producer] = logger
			}
			logger.Warnf("Cloud event time is in the future: now() = %v is before event.time = %v \n %+v", time.Now(), hdrs[i].Time, hdrs[i])
		}
		newMsg := origMsg.Copy()
		setDefaults(&hdrs[i], source, defaultID)
		setMetaData(&hdrs[i], newMsg)
		newMsg.SetStructuredMut(
			&cloudevent.CloudEvent[json.RawMessage]{
				CloudEventHeader: hdrs[i],
				Data:             eventData,
			},
		)
		messages[i] = newMsg
	}

	// Add index and values to the first message without an error only, so we do not get duplicate s3 objects
	for i := range messages {
		if messages[i].GetError() == nil {
			objectKey := clickhouse.CloudEventToObjectKey(&hdrs[i])
			messages[i].MetaSetMut(cloudEventIndexKey, objectKey)
			messages[i].MetaSetMut(CloudEventIndexValueKey, hdrs)
			break
		}
	}

	return messages, nil
}

func setDefaults(event *cloudevent.CloudEventHeader, source, defaultID string) {
	event.Source = source
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	if event.ID == "" {
		event.ID = defaultID
	}
}

func setMetaData(hdr *cloudevent.CloudEventHeader, msg *service.Message) {
	contentType := cloudEventValidContentType
	if !isValidCloudEventHeader(hdr) {
		contentType = cloudEventPartialContentType
	}

	msg.MetaSetMut(processors.MessageContentKey, contentType)
	msg.MetaSetMut(cloudEventTypeKey, hdr.Type)
	msg.MetaSetMut(cloudEventProducerKey, hdr.Producer)
	msg.MetaSetMut(cloudEventSubjectKey, hdr.Subject)
	msg.MetaSetMut(cloudEventIDKey, hdr.ID)
	msg.MetaSetMut(httpinputserver.DIMOCloudEventSource, hdr.Source)
}

func isValidCloudEventHeader(eventHdr *cloudevent.CloudEventHeader) bool {
	if _, err := cloudevent.DecodeNFTDID(eventHdr.Subject); err != nil {
		return false
	}

	if _, err := cloudevent.DecodeNFTDID(eventHdr.Producer); err != nil {
		if _, err = cloudevent.DecodeEthrDID(eventHdr.Producer); err != nil {
			return false
		}
	}

	return common.IsHexAddress(eventHdr.Source)
}
