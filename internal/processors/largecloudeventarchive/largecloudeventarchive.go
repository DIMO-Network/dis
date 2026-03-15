package largecloudeventarchive

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ch "github.com/DIMO-Network/cloudevent/clickhouse"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	processorName = "dimo_large_cloudevent_archive"

	MetaS3UploadKey           = "dimo_s3_upload_key"
	MetaS3ContentType         = "dimo_s3_content_type"
	MetaLargeObjectKey        = "dimo_large_object_key"
	MetaMessageContent        = "dimo_message_content"
	MetaClickHouseCloudEvent  = "dimo_clickhouse_cloudevent"
	DefaultContentType        = "application/json"
	MetricLargeUploads        = "dis_large_cloudevent_s3_uploads_total"
	MetricLargeUploadBytes    = "dis_large_cloudevent_s3_upload_bytes_total"
	MetricLargeUploadFailures = "dis_large_cloudevent_s3_upload_errors_total"
)

var configSpec = service.NewConfigSpec().
	Summary("Stores large CloudEvents as direct JSON objects and emits ClickHouse index rows.").
	Field(service.NewStringField("prefix").Description("Path prefix for object key (e.g. cloudevent/valid/)."))

func init() {
	if err := service.RegisterBatchProcessor(processorName, configSpec, ctor); err != nil {
		panic(err)
	}
}

type processor struct {
	prefix       string
	logger       *service.Logger
	uploads      *service.MetricCounter
	uploadBytes  *service.MetricCounter
	uploadErrors *service.MetricCounter
	nowFn        func() time.Time
}

func ctor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	prefix, err := conf.FieldString("prefix")
	if err != nil {
		return nil, fmt.Errorf("prefix: %w", err)
	}

	m := mgr.Metrics()
	return &processor{
		prefix:       prefix,
		logger:       mgr.Logger(),
		uploads:      m.NewCounter(MetricLargeUploads),
		uploadBytes:  m.NewCounter(MetricLargeUploadBytes),
		uploadErrors: m.NewCounter(MetricLargeUploadFailures),
		nowFn:        func() time.Time { return time.Now().UTC() },
	}, nil
}

func (p *processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{}, nil
	}

	now := p.nowFn()
	out := make(service.MessageBatch, 0, len(msgs)*2)

	for i, msg := range msgs {
		raw, err := msg.AsBytes()
		if err != nil {
			p.logger.Warnf("message %d: get bytes: %v, skipping", i, err)
			continue
		}

		var event cloudevent.RawEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			p.logger.Warnf("message %d: unmarshal cloudevent: %v, skipping", i, err)
			continue
		}

		objectKey := buildObjectKey(p.prefix, now)
		p.uploads.Incr(1)
		p.uploadBytes.Incr(int64(len(raw)))

		s3Msg := service.NewMessage(raw)
		s3Msg.MetaSetMut(MetaS3UploadKey, objectKey)
		s3Msg.MetaSetMut(MetaS3ContentType, DefaultContentType)
		s3Msg.MetaSetMut(MetaLargeObjectKey, objectKey)
		out = append(out, s3Msg)

		chMsg := service.NewMessage(nil)
		chMsg.SetStructured(ch.CloudEventToSliceWithKey(&event.CloudEventHeader, objectKey))
		chMsg.MetaSetMut(MetaMessageContent, MetaClickHouseCloudEvent)
		out = append(out, chMsg)
	}

	if len(out) == 0 {
		return []service.MessageBatch{}, nil
	}

	return []service.MessageBatch{out}, nil
}

func (p *processor) Close(context.Context) error {
	return nil
}

func buildObjectKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/single-%s.json",
		prefix, t.Year(), int(t.Month()), t.Day(), uuid.NewString())
}
