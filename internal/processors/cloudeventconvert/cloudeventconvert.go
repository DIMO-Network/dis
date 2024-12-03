package cloudeventconvert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
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

	cloudEventValidContentType   = "dimo_valid_cloudevent"
	cloudEventPartialContentType = "dimo_valid_cloudevent"
	cloudEventIndexContentType   = "dimo_valid_index_values"
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

		err = updateEventMsg(msg, source, hdrs, eventData)
		if err != nil {
			retBatches = processors.AppendError(retBatches, msg, err)
			continue
		}
		idxValueMsgs, err := createIndexValueMsgs(msg, hdrs)
		if err != nil {
			retBatches = processors.AppendError(retBatches, msg, err)
			continue
		}
		retBatch = append(retBatch, msg)
		retBatch = append(retBatch, idxValueMsgs...)
		retBatches = append(retBatches, retBatch)

	}
	return retBatches, nil
}

func updateEventMsg(origMsg *service.Message, source string, hdrs []cloudevent.CloudEventHeader, eventData []byte) error {
	for i := range hdrs {
		err := setDefaults(&hdrs[i], source)
		if err != nil {
			return err
		}
	}
	// In the future we may have a better way to handle multiple cloud events headers but for now we will just use the first one
	mainHdr := hdrs[0]
	err := SetMetaData(&mainHdr, origMsg)
	if err != nil {
		return err
	}
	origMsg.SetStructuredMut(
		&cloudevent.CloudEvent[json.RawMessage]{
			CloudEventHeader: mainHdr,
			Data:             eventData,
		},
	)
	return nil
}

func createIndexValueMsgs(origMsg *service.Message, hdrs []cloudevent.CloudEventHeader) ([]*service.Message, error) {
	encodedIndex, ok := origMsg.MetaGet(cloudEventIndexKey)
	if !ok {
		return nil, fmt.Errorf("failed to get cloud event index")
	}
	retMsgs := make([]*service.Message, 0, len(hdrs))
	for _, hdr := range hdrs {
		index, _ := getCloudEventIndex(&hdr)
		idxValues := chindexer.IndexToSliceWithKey(&index, encodedIndex)
		newMsg := origMsg.Copy()
		newMsg.MetaSetMut(processors.MessageContentKey, cloudEventIndexContentType)
		newMsg.SetStructuredMut(idxValues)
		retMsgs = append(retMsgs, newMsg)
	}
	return retMsgs, nil
}

func setDefaults(event *cloudevent.CloudEventHeader, source string) error {
	event.Source = source
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	if event.ID == "" {
		event.ID = ksuid.New().String()
	}
	return nil
}

func SetMetaData(eventHeader *cloudevent.CloudEventHeader, msg *service.Message) error {
	allTypes := strings.Split(eventHeader.Type, ",")
	allValues := make([][]any, len(allTypes))
	hdrCpy := *eventHeader

	// get index value of the first message
	eventType := strings.Split(eventHeader.Type, ",")[0]
	hdrCpy.Type = eventType
	index, valid := getCloudEventIndex(&hdrCpy)

	// Encode the index
	encodedIndex, err := nameindexer.EncodeIndex(&index)
	if err != nil {
		return fmt.Errorf("failed to encode index: %w", err)
	}

	for i, eventType := range allTypes {
		hdrCpy.Type = eventType
		index, _ = getCloudEventIndex(&hdrCpy)
		allValues[i] = chindexer.IndexToSliceWithKey(&index, encodedIndex)
	}

	// Set the encoded index and values in the message metadata
	msg.MetaSetMut(cloudEventIndexKey, encodedIndex)
	contentType := cloudEventValidContentType
	if !valid {
		contentType = cloudEventPartialContentType
	}
	msg.MetaSetMut(processors.MessageContentKey, contentType)
	msg.MetaSetMut(cloudEventTypeKey, eventHeader.Type)
	msg.MetaSetMut(cloudEventProducerKey, eventHeader.Producer)
	msg.MetaSetMut(cloudEventSubjectKey, eventHeader.Subject)
	msg.MetaSetMut(cloudEventIDKey, eventHeader.ID)

	return nil
}

// getCloudEventIndexe attempts to convert the cloud event headers to a cloud index if the headers are not in the expected format it will create a partial index
func getCloudEventIndex(eventHdr *cloudevent.CloudEventHeader) (nameindexer.Index, bool) {
	index, err := nameindexer.CloudEventToIndex(eventHdr)
	if err != nil {
		// if the cloud event headers do not match our specific format we will try to create a partial index
		return nameindexer.CloudEventToPartialIndex(eventHdr), false
	}

	return index, true
}
