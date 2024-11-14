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
	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/nameindexer"
	chindexer "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/ksuid"
)

const (
	// CloudEventValidKey is a key used to store whether the message is a valid cloud event.
	CloudEventValidKey       = "dimo_valid_cloudevent"
	cloudEventTypeKey        = "dimo_cloudevent_type"
	cloudEventProducerKey    = "dimo_cloudevent_producer"
	cloudEventSubjectKey     = "dimo_cloudevent_subject"
	cloudEventIDKey          = "dimo_cloudevent_id"
	cloudEventIndexKey       = "dimo_cloudevent_index"
	cloudEventIndexValuesKey = "dimo_cloudevent_index_values"
)

type CloudEventModule interface {
	CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error)
}
type cloudeventProcessor struct {
	cloudEventModule CloudEventModule
	logger           *service.Logger
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

func (c *cloudeventProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		var retBatch service.MessageBatch
		msgBytes, err := msg.AsBytes()
		if err != nil {
			// Add the error to the batch and continue to the next message.
			retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to get msg bytes: %w", err))
			continue
		}

		events, err := c.cloudEventModule.CloudEventConvert(ctx, msgBytes)
		if err != nil {
			// Try to unmarshal convert errors
			errMsg := msg.Copy()
			errMsg.SetError(err)
			data, marshalErr := json.Marshal(err)
			if marshalErr == nil {
				errMsg.SetBytes(data)
			}
			retBatch = append(retBatch, errMsg)
		}

		source, ok := msg.MetaGet(httpinputserver.DIMOConnectionIdKey)
		if !ok {
			retBatches = processors.AppendError(retBatches, msg, fmt.Errorf("failed to get source from connection id"))
			continue
		}

		for _, eventData := range events {
			newMsg, err := c.createNewEventMsg(msg, source, eventData)
			if err != nil {
				retBatches = processors.AppendError(retBatches, msg, err)
				continue
			}
			retBatch = append(retBatch, newMsg)
		}
		retBatches = append(retBatches, retBatch)
	}
	return retBatches, nil
}

func (c *cloudeventProcessor) createNewEventMsg(origMsg *service.Message, source string, eventData []byte) (*service.Message, error) {
	event, err := setDefaults(eventData, source)
	if err != nil {
		return nil, err
	}
	err = c.SetMetaData(&event.CloudEventHeader, origMsg)
	if err != nil {
		return nil, err
	}
	newEventData, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new event: %w", err)
	}
	origMsg.SetBytes(newEventData)
	return origMsg, nil
}

func setDefaults(eventData []byte, source string) (*cloudevent.CloudEvent[json.RawMessage], error) {
	event := cloudevent.CloudEvent[json.RawMessage]{}
	err := json.Unmarshal(eventData, &event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal new event to a cloudEvent: %w", err)
	}
	event.Source = source
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	if event.ID == "" {
		event.ID = ksuid.New().String()
	}
	return &event, nil
}

func (c *cloudeventProcessor) SetMetaData(eventHeader *cloudevent.CloudEventHeader, msg *service.Message) error {
	index, valid := c.getCloudEventIndexes(eventHeader)

	// Encode the index
	encodedIndex, err := nameindexer.EncodeIndex(&index)
	if err != nil {
		return fmt.Errorf("failed to encode index: %w", err)
	}
	indexValues, err := chindexer.IndexToSlice(&index)
	if err != nil {
		return fmt.Errorf("failed to convert index to slice: %w", err)
	}

	// Set the encoded index and values in the message metadata
	msg.MetaSetMut(cloudEventIndexKey, encodedIndex)
	msg.MetaSetMut(CloudEventValidKey, valid)
	msg.MetaSetMut(cloudEventIndexValuesKey, indexValues)
	msg.MetaSetMut(cloudEventTypeKey, eventHeader.Type)
	msg.MetaSetMut(cloudEventProducerKey, eventHeader.Producer)
	msg.MetaSetMut(cloudEventSubjectKey, eventHeader.Subject)
	msg.MetaSetMut(cloudEventIDKey, eventHeader.ID)

	return nil
}

// getCloudEventIndexes attempts to convert the cloud event headers to a cloud index if the headers are not in the expected format it will create a partial index
func (c *cloudeventProcessor) getCloudEventIndexes(eventHdr *cloudevent.CloudEventHeader) (nameindexer.Index, bool) {
	cloudIndex, err := nameindexer.CloudEventToCloudIndex(eventHdr, nameindexer.DefaultSecondaryFiller)
	if err != nil {
		// if the cloud event headers do not match our specific format we will try to create a partial index
		c.logger.Infof("creating partial index, failed to convert cloud event to cloud index: %v", err)
		return nameindexer.CloudEventToPartialIndex(eventHdr, ""), false
	}
	// this does not error due to the above check
	index, _ := cloudIndex.ToIndex()
	return index, true
}
