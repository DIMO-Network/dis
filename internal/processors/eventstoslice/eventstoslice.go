package eventstoslice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type eventSliceProcessor struct {
	logger    *service.Logger
	rowsTotal *service.MetricCounter
	errTotal  *service.MetricCounter
}

func init() {
	configSpec := service.NewConfigSpec().Description("Unpacks EventCloudEvent envelope into flat event row slices for ClickHouse.")
	constructor := func(_ *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		m := mgr.Metrics()
		return &eventSliceProcessor{
			logger:    mgr.Logger(),
			rowsTotal: m.NewCounter(processors.MetricCHInserts, "table"),
			errTotal:  m.NewCounter(processors.MetricCHInsertErrors, "table"),
		}, nil
	}
	err := service.RegisterProcessor("dimo_event_to_slice", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

func (s *eventSliceProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	payload, err := msg.AsBytes()
	if err != nil {
		s.errTotal.Incr(1, processors.TableEvent)
		return nil, fmt.Errorf("failed to get event message payload: %w", err)
	}

	var eventCE vss.EventCloudEvent
	err = json.Unmarshal(payload, &eventCE)
	if err != nil {
		s.errTotal.Incr(1, processors.TableEvent)
		return nil, fmt.Errorf("failed to unmarshal event CloudEvent: %w", err)
	}

	events := vss.UnpackEvents(eventCE)
	if len(events) == 0 {
		return nil, nil
	}
	msgs := make([]*service.Message, 0, len(events))
	for _, e := range events {
		msgCpy := msg.Copy()
		msgCpy.SetStructured(vss.EventToSlice(e))
		msgs = append(msgs, msgCpy)
	}
	s.rowsTotal.Incr(int64(len(msgs)), processors.TableEvent)
	return msgs, nil
}

func (s *eventSliceProcessor) Close(context.Context) error {
	return nil
}
