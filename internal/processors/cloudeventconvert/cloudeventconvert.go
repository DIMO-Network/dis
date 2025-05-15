package cloudeventconvert

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	"github.com/DIMO-Network/dis/internal/ratedlogger"
	"github.com/DIMO-Network/model-garage/pkg/autopi"
	"github.com/DIMO-Network/model-garage/pkg/compass"
	"github.com/DIMO-Network/model-garage/pkg/hashdog"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/ruptela"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redpanda-data/benthos/v4/public/service"
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

var erc1271magicValue = [4]byte{0x16, 0x26, 0xba, 0x7e}
var validCharacters = regexp.MustCompile(`^[a-zA-Z0-9\-_/,. :]+$`)

type cloudeventProcessor struct {
	logger          *service.Logger
	producerLoggers map[string]*ratedlogger.Logger
	ethClient       *ethclient.Client
}

// Close to fulfill the service.Processor interface.
func (*cloudeventProcessor) Close(context.Context) error {
	return nil
}

func newCloudConvertProcessor(client *ethclient.Client, lgr *service.Logger, chainID uint64, vehicleAddr, aftermarketAddr, syntheticAddr common.Address) *cloudeventProcessor {
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
		logger:    lgr,
		ethClient: client,
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
		processors.SetError(msg, processorName, "failed to get source from message metadata", nil)
		return service.MessageBatch{msg}
	}

	contentType, ok := msg.MetaGet(processors.MessageContentKey)
	if !ok {
		processors.SetError(msg, processorName, "failed to get content type from message metadata", nil)
		return service.MessageBatch{msg}
	}

	switch contentType {
	case httpinputserver.ConnectionContent:
		return c.processConnectionMsg(ctx, msg, msgBytes, source)
	case httpinputserver.AttestationContent:
		return c.processAttestationMsg(ctx, msg, msgBytes, source)
	default:
		processors.SetError(msg, processorName, "Internal error", errors.New("unknown content type"))
		return service.MessageBatch{msg}
	}
}

func validateHeadersAndSetDefaults(event *cloudevent.CloudEventHeader, source, defaultID string) error {
	event.Source = source

	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}

	if event.ID == "" {
		event.ID = defaultID
	}
	if event.SpecVersion == "" {
		event.SpecVersion = "1.0"
	}
	if event.DataContentType == "" {
		event.DataContentType = "application/json"
	}

	if !validCharacters.MatchString(event.ID) {
		return fmt.Errorf("invalid id: %s", event.ID)
	}
	if !validCharacters.MatchString(event.SpecVersion) {
		return fmt.Errorf("invalid specversion: %s", event.SpecVersion)
	}
	if !validCharacters.MatchString(event.DataContentType) {
		return fmt.Errorf("invalid data content type: %s", event.DataContentType)
	}
	if event.DataSchema != "" && !validCharacters.MatchString(event.DataSchema) {
		return fmt.Errorf("invalid data schema: %s", event.DataSchema)
	}
	if event.DataVersion != "" && !validCharacters.MatchString(event.DataVersion) {
		return fmt.Errorf("invalid data version: %s", event.DataVersion)
	}
	if event.Type != "" && !validCharacters.MatchString(event.Type) {
		return fmt.Errorf("invalid data type: %s", event.Type)
	}
	if event.Subject != "" && !validCharacters.MatchString(event.Subject) {
		return fmt.Errorf("invalid subject: %s", event.Subject)
	}
	if event.Producer != "" && !validCharacters.MatchString(event.Producer) {
		return fmt.Errorf("invalid producer: %s", event.Producer)
	}

	return nil
}

func setMetaData(hdr *cloudevent.CloudEventHeader, msg *service.Message) {
	msg.MetaSetMut(cloudEventTypeKey, hdr.Type)
	msg.MetaSetMut(cloudEventProducerKey, hdr.Producer)
	msg.MetaSetMut(cloudEventSubjectKey, hdr.Subject)
	msg.MetaSetMut(cloudEventIDKey, hdr.ID)
	msg.MetaSetMut(httpinputserver.DIMOCloudEventSource, hdr.Source)
}
