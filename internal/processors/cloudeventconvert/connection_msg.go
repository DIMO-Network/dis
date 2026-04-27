package cloudeventconvert

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
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
		if err := validateHeadersAndSetDefaults(hdr, source, defaultID, false); err != nil {
			return nil, fmt.Errorf("invalid cloud event header string: %w", err)
		}
		if !isValidConnectionType(hdr) {
			return nil, fmt.Errorf("unsupported cloud event type: %s", hdr.Type)
		}
		setConnectionContentType(hdr, newMsg, c.logger)
		setMetaData(hdr, newMsg)
		newMsg.SetStructuredMut(
			&cloudevent.RawEvent{
				CloudEventHeader: *hdr,
				Data:             eventData,
			},
		)
		messages[i] = newMsg
	}

	return messages, nil
}

func setConnectionContentType(eventHdr *cloudevent.CloudEventHeader, msg *service.Message, logger *service.Logger) {
	if !isValidConnectionHeader(eventHdr, logger) {
		logger.Warnf("invalid cloud event header for header=%+v", eventHdr)
	}
	msg.MetaSetMut(processors.MessageContentKey, cloudEventValidContentType)
}

// isValidConnectionHeader validates a connection cloud event header and
// rewrites Subject, Producer, and Source in place so that any contract or
// account address is in EIP-55 checksum form. Lowercased / mixed-case
// addresses are accepted on input but normalized before being passed
// downstream (metadata, ClickHouse, Kafka, Parquet). Legacy `did:nft:`
// values are also rewritten to their canonical `did:erc721:` form.
func isValidConnectionHeader(eventHdr *cloudevent.CloudEventHeader, logger *service.Logger) bool {
	if did, err := cloudevent.DecodeERC721DID(eventHdr.Subject); err == nil {
		eventHdr.Subject = did.String()
	} else {
		did, err := cloudevent.DecodeLegacyNFTDID(eventHdr.Subject)
		if err != nil {
			return false
		}
		eventHdr.Subject = did.String()
		logger.Debugf("Cloud event header subject for source %s is a legacy NFT DID: %v", eventHdr.Source, eventHdr)
	}

	if did, err := cloudevent.DecodeERC721DID(eventHdr.Producer); err == nil {
		eventHdr.Producer = did.String()
	} else {
		did, err := cloudevent.DecodeLegacyNFTDID(eventHdr.Producer)
		if err != nil {
			return false
		}
		eventHdr.Producer = did.String()
		logger.Debugf("Cloud event header producer for source %s is a legacy NFT DID: %v", eventHdr.Source, eventHdr)
	}

	if !common.IsHexAddress(eventHdr.Source) {
		return false
	}
	eventHdr.Source = common.HexToAddress(eventHdr.Source).Hex()
	return true
}

func isValidConnectionType(eventHdr *cloudevent.CloudEventHeader) bool {
	return eventHdr.Type == cloudevent.TypeStatus || eventHdr.Type == cloudevent.TypeFingerprint || eventHdr.Type == cloudevent.TypeEvents || eventHdr.Type == cloudevent.TypeSignals || eventHdr.Type == cloudevent.TypeRawStatus
}
