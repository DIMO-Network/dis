// Package rawparquet provides a Benthos batch processor that converts a batch of
// CloudEvent messages into a single Parquet message plus the original messages with metadata.
// Adapted from dps dimo_cloudevent_to_parquet processor.
package rawparquet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/clickhouse"
	pq "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// MetaS3UploadKey is the object key for the Parquet file (path within bucket).
	MetaS3UploadKey = "dimo_s3_upload_key"
	// MetaParquetPath is the object key (path) for downstream use.
	MetaParquetPath  = "dimo_parquet_path"
	MetaParquetSize  = "dimo_parquet_size"
	MetaParquetCount = "dimo_parquet_count"
	// MetaMessageContent is the content type key for downstream routing.
	MetaMessageContent = "dimo_message_content"
	// MetaClickHouseCloudEvent is the content value for ClickHouse CE rows.
	MetaClickHouseCloudEvent = "dimo_clickhouse_cloudevent"

	MetricS3Uploads      = "dis_s3_uploads_total"
	MetricS3UploadBytes  = "dis_s3_upload_bytes_total"
	MetricS3UploadErrors = "dis_s3_upload_errors_total"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts a batch of CloudEvents to a single Parquet message with day-partitioned path, plus originals with index metadata.").
	Field(service.NewStringField("prefix").Description("Path prefix for object key (e.g. cloudevent/valid/)."))

func init() {
	err := service.RegisterBatchProcessor("dimo_raw_parquet", configSpec, ctor)
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
		uploads:      m.NewCounter(MetricS3Uploads),
		uploadBytes:  m.NewCounter(MetricS3UploadBytes),
		uploadErrors: m.NewCounter(MetricS3UploadErrors),
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

	type goodMsg struct {
		event cloudevent.RawEvent
		msg   *service.Message
	}
	var good []goodMsg
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
		good = append(good, goodMsg{event: ev, msg: msg})
	}

	if len(good) == 0 {
		return []service.MessageBatch{}, nil
	}

	// Deduplicate events by ID for parquet encoding, preferring dimo.status.
	// Status and fingerprint may share the same ID; we store one parquet entry
	// per ID but create ClickHouse index rows for all events.
	parquetEvents := make([]cloudevent.RawEvent, 0, len(good))
	parquetIdx := make([]int, len(good))
	seenID := make(map[string]int, len(good))

	for i, g := range good {
		if idx, dup := seenID[g.event.ID]; dup {
			parquetIdx[i] = idx
			if g.event.Type == "dimo.status" {
				parquetEvents[idx] = g.event
			}
		} else {
			parquetIdx[i] = len(parquetEvents)
			seenID[g.event.ID] = len(parquetEvents)
			parquetEvents = append(parquetEvents, g.event)
		}
	}

	now := time.Now().UTC()
	objectKey := buildObjectKey(p.prefix, now)

	var buf bytes.Buffer
	indexKeyMap, err := pq.Encode(&buf, parquetEvents, objectKey)
	if err != nil {
		p.uploadErrors.Incr(1)
		return nil, fmt.Errorf("encode parquet: %w", err)
	}

	parquetBytes := buf.Bytes()
	p.uploads.Incr(1)
	p.uploadBytes.Incr(int64(len(parquetBytes)))
	parquetMsg := service.NewMessage(parquetBytes)
	parquetMsg.MetaSetMut(MetaS3UploadKey, objectKey)
	parquetMsg.MetaSetMut(MetaParquetPath, objectKey)
	parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(len(parquetBytes)))
	parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(len(parquetEvents)))

	out := make(service.MessageBatch, 0, 1+len(good))
	out = append(out, parquetMsg)
	for i, g := range good {
		chMsg := service.NewMessage(nil)
		row := clickhouse.CloudEventToSliceWithKey(&g.event.CloudEventHeader, indexKeyMap[parquetIdx[i]])
		chMsg.SetStructured(row)
		chMsg.MetaSetMut(MetaMessageContent, MetaClickHouseCloudEvent)
		out = append(out, chMsg)
	}
	return []service.MessageBatch{out}, nil
}

func buildObjectKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/batch-%s.parquet",
		prefix, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}
