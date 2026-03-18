// Package rawdocument provides a Benthos batch processor that stores large
// CloudEvent documents as individual S3 objects and emits ClickHouse index rows.
package rawdocument

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/clickhouse"
	"github.com/DIMO-Network/dis/internal/processors/rawparquet"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	MetricDocUploads      = "dis_s3_doc_uploads_total"
	MetricDocUploadBytes  = "dis_s3_doc_upload_bytes_total"
	MetricDocUploadErrors = "dis_s3_doc_upload_errors_total"
)

var configSpec = service.NewConfigSpec().
	Summary("Stores large CloudEvent documents as individual S3 objects with ClickHouse index rows.").
	Field(service.NewStringField("prefix").Description("Path prefix for object key (e.g. cloudevent/blobs/)."))

func init() {
	err := service.RegisterBatchProcessor("dimo_raw_document", configSpec, ctor)
	if err != nil {
		panic(err)
	}
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
		uploads:      m.NewCounter(MetricDocUploads),
		uploadBytes:  m.NewCounter(MetricDocUploadBytes),
		uploadErrors: m.NewCounter(MetricDocUploadErrors),
	}, nil
}

type processor struct {
	prefix       string
	logger       *service.Logger
	uploads      *service.MetricCounter
	uploadBytes  *service.MetricCounter
	uploadErrors *service.MetricCounter
}

func (p *processor) Close(context.Context) error { return nil }

func (p *processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{}, nil
	}

	out := make(service.MessageBatch, 0, len(msgs)*2)
	for i, msg := range msgs {
		b, err := msg.AsBytes()
		if err != nil {
			p.logger.Warnf("message %d: get bytes: %v, skipping", i, err)
			continue
		}
		var ev cloudevent.RawEvent
		if err := json.Unmarshal(b, &ev); err != nil {
			p.logger.Warnf("message %d: unmarshal cloudevent: %v, skipping", i, err)
			continue
		}

		now := time.Now().UTC()
		s3Key := buildObjectKey(p.prefix, ev.Subject, now)

		// S3 upload message: full CloudEvent JSON.
		s3Msg := service.NewMessage(b)
		s3Msg.MetaSetMut(rawparquet.MetaS3UploadKey, s3Key)

		// ClickHouse index row.
		chMsg := service.NewMessage(nil)
		row := clickhouse.CloudEventToSliceWithKey(&ev.CloudEventHeader, s3Key)
		chMsg.SetStructured(row)
		chMsg.MetaSetMut(rawparquet.MetaMessageContent, rawparquet.MetaClickHouseCloudEvent)

		p.uploads.Incr(1)
		p.uploadBytes.Incr(int64(len(b)))
		out = append(out, s3Msg, chMsg)
	}

	if len(out) == 0 {
		return []service.MessageBatch{}, nil
	}
	return []service.MessageBatch{out}, nil
}

func buildObjectKey(prefix, subject string, t time.Time) string {
	return fmt.Sprintf("%s%s/%d/%02d/%02d/single-%s.json",
		prefix, subject, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}
