package cloudeventconvert

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/pkg/clickhouse"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/ratedlogger"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/ksuid"
)

func (c *cloudeventProcessor) processConnectionMsg(ctx context.Context, msg *service.Message, msgBytes []byte, source string) service.MessageBatch {
	hdrs, eventData, err := modules.ConvertToCloudEvents(ctx, source, msgBytes)
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
		// If the module chooses not to return data, use the original message
		eventData = msgBytes
	}
	retBatch, err := c.createConnectionMsgs(msg, source, hdrs, eventData)
	if err != nil {
		processors.SetError(msg, processorName, "failed to create event messages", err)
		return service.MessageBatch{msg}
	}
	return retBatch
}

func (c *cloudeventProcessor) createConnectionMsgs(origMsg *service.Message, source string, hdrs []cloudevent.CloudEventHeader, eventData []byte) ([]*service.Message, error) {
	if len(hdrs) == 0 {
		return nil, fmt.Errorf("no cloud events headers returned")
	}
	messages := make([]*service.Message, len(hdrs))
	defaultID := ksuid.New().String()
	// set defaults and metadata for each header, then create a message for each header
	for i := range hdrs {
		hdr := &hdrs[i]
		if processors.IsFutureTimestamp(hdr.Time) {
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
		if err := validateHeadersAndSetDefaults(hdr, source, defaultID); err != nil {
			return nil, fmt.Errorf("invalid cloud event header string: %w", err)
		}
		if !isValidConnectionType(hdr) {
			return nil, fmt.Errorf("unsupported cloud event type: %s", hdr.Type)
		}
		setConnectionContentType(hdr, newMsg, c.logger)
		setMetaData(hdr, newMsg)
		newMsg.SetStructuredMut(
			&cloudevent.CloudEvent[json.RawMessage]{
				CloudEventHeader: *hdr,
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

func setConnectionContentType(eventHdr *cloudevent.CloudEventHeader, msg *service.Message, logger *service.Logger) {
	contentType := cloudEventValidContentType
	if !isValidConnectionHeader(eventHdr, logger) {
		contentType = cloudEventPartialContentType
	}
	msg.MetaSetMut(processors.MessageContentKey, contentType)
}

func isValidConnectionHeader(eventHdr *cloudevent.CloudEventHeader, logger *service.Logger) bool {
	if _, err := cloudevent.DecodeERC721DID(eventHdr.Subject); err != nil {
		did, err := cloudevent.DecodeLegacyNFTDID(eventHdr.Subject)
		if err != nil {
			return false
		}
		eventHdr.Subject = did.String()
		logger.Warnf("Cloud event header subject for source %s is a legacy NFT DID: %v", eventHdr.Source, eventHdr)
	}

	if _, err := cloudevent.DecodeERC721DID(eventHdr.Producer); err != nil {
		did, err := cloudevent.DecodeLegacyNFTDID(eventHdr.Producer)
		if err != nil {
			return false
		}
		eventHdr.Producer = did.String()
		logger.Warnf("Cloud event header producer for source %s is a legacy NFT DID: %v", eventHdr.Source, eventHdr)
	}

	return common.IsHexAddress(eventHdr.Source)
}

func isValidConnectionType(eventHdr *cloudevent.CloudEventHeader) bool {
	return eventHdr.Type == cloudevent.TypeStatus || eventHdr.Type == cloudevent.TypeFingerprint
}
