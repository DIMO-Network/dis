// Package rawparquet provides a Benthos batch processor that converts a batch of
// CloudEvent messages into a single Parquet message plus the original messages with metadata.
// Adapted from dps dimo_cloudevent_to_parquet processor.
package rawparquet

import (
	"bytes"
	"context"
	"encoding/base64"
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
	// emitted by this processor — both parquet bundles and per-event data
	// blobs use this metadata key.
	MetaS3UploadKey = "dimo_s3_upload_key"
	// MetaS3ContentType drives the S3 output's Content-Type per message.
	// Parquet bundles set "application/octet-stream"; data blobs set the
	// event's datacontenttype.
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
	MetricDataExternals  = "dis_s3_data_externals_total"
	MetricDataExternalBy = "dis_s3_data_external_bytes_total"

	contentTypeOctetStream = "application/octet-stream"
)

var configSpec = service.NewConfigSpec().
	Summary("Converts a batch of CloudEvents to a single Parquet message with day-partitioned path, plus originals with index metadata.").
	Field(service.NewStringField("prefix").Description("Path prefix for the parquet object key (e.g. cloudevent/valid/).")).
	Field(service.NewStringField("data_prefix").Default("cloudevent/blobs/").Description("Path prefix for externalized per-event data blobs (e.g. cloudevent/blobs/).")).
	Field(service.NewIntField("data_size_threshold").Default(1 << 20).Description("Payload byte threshold above which the event's data is externalized to its own S3 object."))

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
	dataPrefix, err := conf.FieldString("data_prefix")
	if err != nil {
		return nil, fmt.Errorf("data_prefix: %w", err)
	}
	threshold, err := conf.FieldInt("data_size_threshold")
	if err != nil {
		return nil, fmt.Errorf("data_size_threshold: %w", err)
	}
	m := mgr.Metrics()
	return &processor{
		prefix:           prefix,
		dataPrefix:       dataPrefix,
		dataSizeThresh:   threshold,
		logger:           mgr.Logger(),
		uploads:          m.NewCounter(MetricS3Uploads),
		uploadBytes:      m.NewCounter(MetricS3UploadBytes),
		uploadErrors:     m.NewCounter(MetricS3UploadErrors),
		dataExternals:    m.NewCounter(MetricDataExternals),
		dataExternalSize: m.NewCounter(MetricDataExternalBy),
	}, nil
}

type processor struct {
	prefix           string
	dataPrefix       string
	dataSizeThresh   int
	logger           *service.Logger
	uploads          *service.MetricCounter
	uploadBytes      *service.MetricCounter
	uploadErrors     *service.MetricCounter
	dataExternals    *service.MetricCounter
	dataExternalSize *service.MetricCounter
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

	// Externalize big payloads. For each unique parquet event, decide whether
	// to split the data off into its own S3 object. Builds a parallel slice
	// of StoredEvents (with empty Data/DataBase64 + DataIndexKey) for parquet
	// encoding, plus a slice of S3 messages to emit for the data PUTs.
	stored := make([]cloudevent.StoredEvent, len(parquetEvents))
	var dataMsgs []*service.Message
	for i := range parquetEvents {
		ev := parquetEvents[i]
		payloadLen := dataPayloadLen(&ev)
		if payloadLen <= p.dataSizeThresh {
			stored[i] = cloudevent.StoredEvent{RawEvent: ev}
			continue
		}
		raw, err := decodePayload(&ev)
		if err != nil {
			p.logger.Warnf("event %s: decode data_base64: %v; keeping inline", ev.ID, err)
			stored[i] = cloudevent.StoredEvent{RawEvent: ev}
			continue
		}
		dataKey := buildDataKey(p.dataPrefix, ev.Subject, now)
		ct := ev.DataContentType
		if ct == "" {
			ct = contentTypeOctetStream
		}
		dataMsg := service.NewMessage(raw)
		dataMsg.MetaSetMut(MetaS3UploadKey, dataKey)
		dataMsg.MetaSetMut(MetaS3ContentType, ct)
		dataMsgs = append(dataMsgs, dataMsg)

		strippedHdr := ev.CloudEventHeader
		stored[i] = cloudevent.StoredEvent{
			RawEvent:     cloudevent.RawEvent{CloudEventHeader: strippedHdr},
			DataIndexKey: dataKey,
		}
		p.dataExternals.Incr(1)
		p.dataExternalSize.Incr(int64(len(raw)))
	}

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
	parquetMsg.MetaSetMut(MetaS3ContentType, contentTypeOctetStream)
	parquetMsg.MetaSetMut(MetaParquetPath, objectKey)
	parquetMsg.MetaSetMut(MetaParquetSize, strconv.Itoa(len(parquetBytes)))
	parquetMsg.MetaSetMut(MetaParquetCount, strconv.Itoa(len(stored)))

	out := make(service.MessageBatch, 0, 1+len(dataMsgs)+len(good))
	out = append(out, parquetMsg)
	out = append(out, dataMsgs...)
	for i, g := range good {
		_ = g
		chMsg := service.NewMessage(nil)
		row := clickhouse.StoredEventToSlice(&stored[parquetIdx[i]], indexKeyMap[parquetIdx[i]])
		chMsg.SetStructured(row)
		chMsg.MetaSetMut(MetaMessageContent, MetaClickHouseCloudEvent)
		out = append(out, chMsg)
	}
	return []service.MessageBatch{out}, nil
}

// dataPayloadLen returns the effective size of an event's payload — decoded
// length for data_base64, raw byte count for inline data.
func dataPayloadLen(ev *cloudevent.RawEvent) int {
	if ev.DataBase64 != "" {
		return base64.StdEncoding.DecodedLen(len(ev.DataBase64))
	}
	return len(ev.Data)
}

// decodePayload returns the raw bytes that should be persisted to S3:
// base64-decoded if data_base64 is set, otherwise the inline data bytes.
func decodePayload(ev *cloudevent.RawEvent) ([]byte, error) {
	if ev.DataBase64 != "" {
		return base64.StdEncoding.DecodeString(ev.DataBase64)
	}
	return []byte(ev.Data), nil
}

func buildObjectKey(prefix string, t time.Time) string {
	return fmt.Sprintf("%s%d/%02d/%02d/batch-%s.parquet",
		prefix, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}

func buildDataKey(prefix, subject string, t time.Time) string {
	return fmt.Sprintf("%s%s/%d/%02d/%02d/%s",
		prefix, subject, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}
