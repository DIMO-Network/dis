package signalstoslice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type sliceProcessor struct {
	logger    *service.Logger
	rowsTotal *service.MetricCounter
	errTotal  *service.MetricCounter
}

func init() {
	configSpec := service.NewConfigSpec().Description("Unpacks SignalCloudEvent envelope into flat signal row slices for ClickHouse.")
	constructor := func(_ *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		m := mgr.Metrics()
		return &sliceProcessor{
			logger:    mgr.Logger(),
			rowsTotal: m.NewCounter(processors.MetricCHInserts, "table"),
			errTotal:  m.NewCounter(processors.MetricCHInsertErrors, "table"),
		}, nil
	}
	err := service.RegisterProcessor("dimo_signal_to_slice", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

func (s *sliceProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	payload, err := msg.AsBytes()
	if err != nil {
		s.errTotal.Incr(1, processors.TableSignal)
		return nil, fmt.Errorf("failed to get signal message payload: %w", err)
	}

	var signalCE vss.SignalCloudEvent
	err = json.Unmarshal(payload, &signalCE)
	if err != nil {
		s.errTotal.Incr(1, processors.TableSignal)
		return nil, fmt.Errorf("failed to unmarshal signal CloudEvent: %w", err)
	}

	signals := vss.UnpackSignals(signalCE)
	if len(signals) == 0 {
		return nil, nil
	}
	msgs := make([]*service.Message, 0, len(signals))
	for _, sig := range signals {
		msgCpy := msg.Copy()
		msgCpy.SetStructured(vss.SignalToSlice(sig))
		msgs = append(msgs, msgCpy)
	}
	s.rowsTotal.Incr(int64(len(msgs)), processors.TableSignal)
	return msgs, nil
}

func (s *sliceProcessor) Close(context.Context) error {
	return nil
}
