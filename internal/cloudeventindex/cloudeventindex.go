package cloudeventconvert

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/nameindexer"
	chindexer "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cloudEventIndexKey       = "dimo_cloudevent_index"
	cloudEventIndexValuesKey = "dimo_cloudevent_index_values"
	cloudeventIndexTypeKey   = "dimo_cloudevent_index_type"
	fullIndexValue           = "full"
	partialIndexValue        = "partial"
)

type eventIndexProcessor struct {
	logger *service.Logger
}

// Close to fulfill the service.Processor interface.
func (*eventIndexProcessor) Close(context.Context) error {
	return nil
}

func (v *eventIndexProcessor) ProcessBatch(ctx context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	var retBatches []service.MessageBatch
	for _, msg := range msgs {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			retBatches = appendError(retBatches, msg, fmt.Errorf("failed to get msg bytes: %w", err))
			continue
		}
		var cloudHeader cloudevent.CloudEventHeader
		err = json.Unmarshal(msgBytes, &cloudHeader)
		if err != nil {
			retBatches = appendError(retBatches, msg, fmt.Errorf("failed to unmarshal cloud event header: %w", err))
			continue
		}
		indexType := fullIndexValue
		index, err := getCloudEventIndexes(&cloudHeader)
		if err != nil {
			// if the cloud event headers do not match our specific format we will try to create a partial index
			v.logger.Infof("creating partial index, failed to convert cloud event to cloud index: %v", err)
			index = nameindexer.CloudEventToPartialIndex(&cloudHeader, "")
			indexType = partialIndexValue
		}

		// Encode the index
		encodedIndex, err := nameindexer.EncodeIndex(&index)
		if err != nil {
			retBatches = appendError(retBatches, msg, fmt.Errorf("failed to encode index: %w", err))
			continue
		}

		// Set the encoded index in the message metadata
		msg.MetaSetMut(cloudEventIndexKey, encodedIndex)
		msg.MetaSetMut(cloudeventIndexTypeKey, indexType)
		indexValues, err := chindexer.IndexToSlice(&index)
		if err != nil {
			return nil, fmt.Errorf("failed to convert index to slice: %w", err)
		}
		msg.MetaSetMut(cloudEventIndexValuesKey, indexValues)

		retBatches = append(retBatches, service.MessageBatch{msg})
	}
	return retBatches, nil
}

func appendError(batches []service.MessageBatch, msg *service.Message, err error) []service.MessageBatch {
	errMsg := msg.Copy()
	errMsg.SetError(err)
	return append(batches, service.MessageBatch{errMsg})
}

func getCloudEventIndexes(cloudEventHeader *cloudevent.CloudEventHeader) (nameindexer.Index, error) {
	cloudIndex, err := nameindexer.CloudEventToCloudIndex(cloudEventHeader, "")
	if err != nil {
		return nameindexer.Index{}, fmt.Errorf("failed to convert cloud event to cloudEvent index: %w", err)
	}
	index, err := cloudIndex.ToIndex()
	if err != nil {
		return nameindexer.Index{}, fmt.Errorf("failed to convert cloudEvent index to standard index: %w", err)
	}
	return index, nil
}
