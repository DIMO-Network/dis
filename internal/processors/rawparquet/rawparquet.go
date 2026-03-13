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
	// MetaS3ContentType is the object content type for downstream S3 uploads.
	MetaS3ContentType = "dimo_s3_content_type"
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
	Field(service.NewStringField("prefix").Description("Path prefix for object key (e.g. cloudevent/valid/).")).
	Field(service.NewIntField("large_event_threshold").
		Description("If an event's byte size meets or exceeds this number then it will be stored as an individual S3 object. 0 disables.").
		Default(0)) // Benthos convention seems to be to not name these _bytes.

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
	largeEventThreshold, err := conf.FieldInt("large_event_threshold")
	if err != nil {
		return nil, fmt.Errorf("large_event_threshold: %w", err)
	}
	m := mgr.Metrics()
	return &processor{
		prefix:              prefix,
		largeEventThreshold: largeEventThreshold,
		logger:              mgr.Logger(),
		uploads:             m.NewCounter(MetricS3Uploads),
		uploadBytes:         m.NewCounter(MetricS3UploadBytes),
		uploadErrors:        m.NewCounter(MetricS3UploadErrors),
	}, nil
}

type processor struct {
	prefix              string
	largeEventThreshold int
	logger              *service.Logger
	uploads             *service.MetricCounter
	uploadBytes         *service.MetricCounter
	uploadErrors        *service.MetricCounter
}

func (p *processor) Close(context.Context) error { return nil }

func (p *processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{}, nil
	}

	type goodMsg struct {
		event    cloudevent.RawEvent
		rawBytes []byte
	}
	var small, large []goodMsg
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
		g := goodMsg{event: ev, rawBytes: b}
		if p.largeEventThreshold > 0 && len(g.rawBytes) >= p.largeEventThreshold {
			large = append(large, g)
			continue
		}
		small = append(small, g)
	}

	if len(small) == 0 && len(large) == 0 {
		return []service.MessageBatch{}, nil
	}

	now := time.Now().UTC()
	out := make(service.MessageBatch, 0, 1+len(small)+2*len(large))

	if len(small) > 0 {
		events := make([]cloudevent.RawEvent, len(small))
		for i, g := range small {
			events[i] = g.event
		}

		objectKey := buildBatchObjectKey(p.prefix, now)
		var buf bytes.Buffer
		indexKeyMap, err := pq.Encode(&buf, events, objectKey)
		if err != nil {
			p.uploadErrors.Incr(1)
			return nil, fmt.Errorf("encode parquet: %w", err)
		}

		parquetBytes := buf.Bytes()
		p.uploads.Incr(1)
		p.uploadBytes.Incr(int64(len(parquetBytes)))
		parquetMsg := service.NewMessage(parquetBytes)
		parquetMsg.MetaSetMut(MetaS3UploadKey, objectKey)
		parquetMsg.MetaSetMut(MetaS3ContentType, "application/octet-stream")
		parquetMsg.MetaSetMut(MetaParquetPath, objectKey)
		parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(len(parquetBytes)))
		parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(len(small)))
		out = append(out, parquetMsg)

		for i, g := range small {
			chMsg := service.NewMessage(nil)
			row := clickhouse.CloudEventToSliceWithKey(&g.event.CloudEventHeader, indexKeyMap[i])
			chMsg.SetStructured(row)
			chMsg.MetaSetMut(MetaMessageContent, MetaClickHouseCloudEvent)
			out = append(out, chMsg)
		}
	}

	for _, g := range large {
		singleKey := buildSingleObjectKey(p.prefix, now)
		p.uploads.Incr(1)
		p.uploadBytes.Incr(int64(len(g.rawBytes)))

		s3Msg := service.NewMessage(g.rawBytes)
		s3Msg.MetaSetMut(MetaS3UploadKey, singleKey)
		s3Msg.MetaSetMut(MetaS3ContentType, "application/json")
		out = append(out, s3Msg)

		chMsg := service.NewMessage(nil)
		row := clickhouse.CloudEventToSliceWithKey(&g.event.CloudEventHeader, singleKey)
		chMsg.SetStructured(row)
		chMsg.MetaSetMut(MetaMessageContent, MetaClickHouseCloudEvent)
		out = append(out, chMsg)
	}
	return []service.MessageBatch{out}, nil
}

func buildBatchObjectKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/batch-%s.parquet",
		prefix, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}

func buildSingleObjectKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/single-%s.json",
		prefix, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}
