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

		retBatch, err := createEventMsgs(msg, source, hdrs, eventData)
		if err != nil {
			retBatches = processors.AppendError(retBatches, msg, err)
			continue
		}

		retBatches = append(retBatches, retBatch)
	}
	return retBatches, nil
}

func createEventMsgs(origMsg *service.Message, source string, hdrs []cloudevent.CloudEventHeader, eventData []byte) ([]*service.Message, error) {
	messages := make([]*service.Message, len(hdrs))
	allValues := make([][]any, len(hdrs))

	var encodedIndex string
	// First set defaults and calculate indices for all headers
	for i := range hdrs {
		newMsg := origMsg.Copy()
		setDefaults(&hdrs[i], source)

		index, valid := getCloudEventIndex(&hdrs[i])
		if encodedIndex == "" {
			var err error
			encodedIndex, err = nameindexer.EncodeIndex(&index)
			if err != nil {
				return nil, fmt.Errorf("failed to encode index: %w", err)
			}
		}
		allValues[i] = chindexer.IndexToSliceWithKey(&index, encodedIndex)

		setMetaData(&hdrs[i], newMsg, valid)
		newMsg.SetStructuredMut(
			&cloudevent.CloudEvent[json.RawMessage]{
				CloudEventHeader: hdrs[i],
				Data:             eventData,
			},
		)
		messages[i] = newMsg
	}

	// Add index and values to the first message only so we do not get duplicate s3 objects
	messages[0].MetaSetMut(cloudEventIndexKey, encodedIndex)
	messages[0].MetaSetMut(CloudEventIndexValueKey, allValues)

	return messages, nil
}

func setDefaults(event *cloudevent.CloudEventHeader, source string) {
	event.Source = source
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	if event.ID == "" {
		event.ID = ksuid.New().String()
	}
}

func setMetaData(hdr *cloudevent.CloudEventHeader, msg *service.Message, valid bool) {
	contentType := cloudEventValidContentType
	if !valid {
		contentType = cloudEventPartialContentType
	}

	msg.MetaSetMut(processors.MessageContentKey, contentType)
	msg.MetaSetMut(cloudEventTypeKey, hdr.Type)
	msg.MetaSetMut(cloudEventProducerKey, hdr.Producer)
	msg.MetaSetMut(cloudEventSubjectKey, hdr.Subject)
	msg.MetaSetMut(cloudEventIDKey, hdr.ID)
}

// getCloudEventIndex attempts to convert the cloud event headers to a cloud index if the headers are not in the expected format it will create a partial index.
func getCloudEventIndex(eventHdr *cloudevent.CloudEventHeader) (nameindexer.Index, bool) {
	index, err := nameindexer.CloudEventToIndex(eventHdr)
	if err != nil {
		// if the cloud event headers do not match our specific format we will try to create a partial index
		return nameindexer.CloudEventToPartialIndex(eventHdr), false
	}

	return index, true
}
