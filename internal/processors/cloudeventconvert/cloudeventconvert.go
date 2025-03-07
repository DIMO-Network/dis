package cloudeventconvert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/dis/internal/modules"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	"github.com/DIMO-Network/dis/internal/ratedlogger"
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/nameindexer"
	"github.com/ethereum/go-ethereum/common"
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

type CloudEventModule interface {
	CloudEventConvert(ctx context.Context, msgData []byte) ([]cloudevent.CloudEventHeader, []byte, error)
}
type cloudeventProcessor struct {
	cloudEventModule CloudEventModule
	logger           *service.Logger
	producerLoggers  map[string]*ratedlogger.Logger
}

// Close to fulfill the service.Processor interface.
func (*cloudeventProcessor) Close(context.Context) error {
	return nil
}

func newCloudConvertProcessor(lgr *service.Logger, moduleName, moduleConfig string) (*cloudeventProcessor, error) {
	decodedModuelConfig, err := base64.StdEncoding.DecodeString(moduleConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decode module config: %w", err)
	}
	moduleOpts := modules.Options{
		Logger:       lgr,
		FilePath:     "",
		ModuleConfig: string(decodedModuelConfig),
	}
	cloudEventModule, err := modules.LoadCloudEventModule(moduleName, moduleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to load signal module: %w", err)
	}
	return &cloudeventProcessor{
		cloudEventModule: cloudEventModule,
		logger:           lgr,
	}, nil
}

// ProcessBatch converts a batch of messages to cloud events.
func (c *cloudeventProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	retBatches := make([]service.MessageBatch, 0, len(msgs))
	for _, msg := range msgs {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			// Add the error to the batch and continue to the next message.
			retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to get msg bytes: %w", err))
			continue
		}

		source, ok := msg.MetaGet(httpinputserver.DIMOConnectionIdKey)
		if !ok {
			retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to get source from connection id"))
			continue
		}

		hdrs, eventData, err := c.cloudEventModule.CloudEventConvert(ctx, msgBytes)
		if err != nil {
			// Try to unmarshal convert errors
			data, marshalErr := json.Marshal(err)
			if marshalErr == nil {
				msg.SetBytes(data)
			}
			retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to convert to cloud event: %w", err))
			continue
		}
		if len(hdrs) == 0 {
			retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("no cloud events headers returned"))
			continue
		}
		if len(eventData) == 0 {
			// If the module chooses not to return data, use the original message will be used
			eventData = msgBytes
		}

		retBatch, err := c.createEventMsgs(msg, source, hdrs, eventData)
		if err != nil {
			retBatches = processors.AppendError(retBatches, msg, err)
			continue
		}

		retBatches = append(retBatches, retBatch)
	}
	return retBatches, nil
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
			objectKey := nameindexer.CloudEventToIndexKey(&hdrs[i])
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
}

func isValidCloudEventHeader(eventHdr *cloudevent.CloudEventHeader) bool {
	if _, err := cloudevent.DecodeNFTDID(eventHdr.Subject); err != nil {
		return false
	}
	if _, err := cloudevent.DecodeNFTDID(eventHdr.Producer); err != nil {
		return false
	}
	if !common.IsHexAddress(eventHdr.Source) {
		return false
	}
	return true
}
