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
	// MetaS3UploadKey is the object key (path within bucket) for any S3 PUT
	// emitted by this processor or by the splitter — both parquet bundles and
	// per-event data blobs use this metadata key.
	MetaS3UploadKey = "dimo_s3_upload_key"
	// MetaS3ContentType drives the blob stream's S3 Content-Type per message.
	// Set by the splitter to the event's datacontenttype.
	MetaS3ContentType = "dimo_s3_content_type"
	// MetaDataIndexKey, when present on an inbound CE message, is treated as
	// the precomputed blob S3 key for that event and is stored in the Parquet
	// row's data_index_key column. The splitter sets this on stripped events;
	// inline events leave it unset.
	MetaDataIndexKey = "dimo_data_index_key"
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
	Field(service.NewStringField("prefix").Description("Path prefix for the parquet object key (e.g. cloudevent/valid/)."))

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

	var good []cloudevent.StoredEvent
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
		dataKey, _ := msg.MetaGet(MetaDataIndexKey)
		good = append(good, cloudevent.StoredEvent{RawEvent: ev, DataIndexKey: dataKey})
	}

	if len(good) == 0 {
		return []service.MessageBatch{}, nil
	}

	// Deduplicate events by ID for parquet encoding, preferring dimo.status.
	// Status and fingerprint may share the same ID; we store one parquet entry
	// per ID but create ClickHouse index rows for all events.
	stored := make([]cloudevent.StoredEvent, 0, len(good))
	parquetIdx := make([]int, len(good))
	seenID := make(map[string]int, len(good))

	for i, g := range good {
		if idx, dup := seenID[g.ID]; dup {
			parquetIdx[i] = idx
			if g.Type == "dimo.status" {
				stored[idx] = g
			}
		} else {
			parquetIdx[i] = len(stored)
			seenID[g.ID] = len(stored)
			stored = append(stored, g)
		}
	}

	now := time.Now().UTC()
	objectKey := buildObjectKey(p.prefix, now)

	var buf bytes.Buffer
	indexKeyMap, err := pq.Encode(&buf, stored, objectKey)
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
	parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(len(stored)))

	out := make(service.MessageBatch, 0, 1+len(good))
	out = append(out, parquetMsg)
	for i := range good {
		chMsg := service.NewMessage(nil)
		row := clickhouse.StoredEventToSlice(&stored[parquetIdx[i]], indexKeyMap[parquetIdx[i]])
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
