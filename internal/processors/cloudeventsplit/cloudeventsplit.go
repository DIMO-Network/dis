// Package cloudeventsplit splits a CloudEvent whose `data` payload exceeds a
// configured threshold into two messages: the original event with `data` and
// `data_base64` cleared (carrying the future blob's S3 key in metadata), and a
// new message containing the raw payload bytes destined for the blob bucket.
package cloudeventsplit

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors/rawparquet"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// MetaBlobMessageContent is the dimo_message_content value set on the blob
	// message produced by the splitter.
	MetaBlobMessageContent = "dimo_blob"

	contentTypeOctetStream = "application/octet-stream"

	MetricExternalized      = "dis_split_externalized_total"
	MetricExternalizedBytes = "dis_split_externalized_bytes_total"
)

var configSpec = service.NewConfigSpec().
	Summary("Splits CloudEvents with large `data` payloads: emits the event with data stripped (and a data_index_key in metadata) plus a separate blob message for the raw payload bytes.").
	Field(service.NewStringField("prefix").Default("cloudevent/blobs/").Description("Path prefix for externalized blob object keys (e.g. cloudevent/blobs/).")).
	Field(service.NewIntField("data_size_threshold").Default(1 << 20).Description("Threshold in bytes against the event's `data` payload size; events at or below this stay inline."))

func init() {
	if err := service.RegisterBatchProcessor("dimo_cloudevent_split", configSpec, ctor); err != nil {
		panic(err)
	}
}

func ctor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	prefix, err := conf.FieldString("prefix")
	if err != nil {
		return nil, fmt.Errorf("prefix: %w", err)
	}
	threshold, err := conf.FieldInt("data_size_threshold")
	if err != nil {
		return nil, fmt.Errorf("data_size_threshold: %w", err)
	}
	m := mgr.Metrics()
	return &processor{
		prefix:           prefix,
		threshold:        threshold,
		logger:           mgr.Logger(),
		externalized:     m.NewCounter(MetricExternalized),
		externalizedSize: m.NewCounter(MetricExternalizedBytes),
	}, nil
}

type processor struct {
	prefix           string
	threshold        int
	logger           *service.Logger
	externalized     *service.MetricCounter
	externalizedSize *service.MetricCounter
}

func (p *processor) Close(context.Context) error { return nil }

func (p *processor) ProcessBatch(_ context.Context, msgs service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msgs) == 0 {
		return []service.MessageBatch{}, nil
	}
	out := make(service.MessageBatch, 0, len(msgs))
	for _, msg := range msgs {
		split, err := p.splitOne(msg)
		if err != nil {
			p.logger.Warnf("split: %v; passing through", err)
			out = append(out, msg)
			continue
		}
		out = append(out, split...)
	}
	return []service.MessageBatch{out}, nil
}

func (p *processor) splitOne(msg *service.Message) (service.MessageBatch, error) {
	b, err := msg.AsBytes()
	if err != nil {
		return service.MessageBatch{msg}, nil
	}
	var ev cloudevent.RawEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		return service.MessageBatch{msg}, nil
	}

	payloadLen := dataPayloadLen(&ev)
	if payloadLen <= p.threshold {
		return service.MessageBatch{msg}, nil
	}

	raw, err := decodePayload(&ev)
	if err != nil {
		return nil, fmt.Errorf("decode data_base64 for event %s: %w", ev.ID, err)
	}

	key := buildBlobKey(p.prefix, ev.Subject, time.Now().UTC())

	stripped := cloudevent.RawEvent{CloudEventHeader: ev.CloudEventHeader}
	strippedBytes, err := json.Marshal(stripped)
	if err != nil {
		return nil, fmt.Errorf("marshal stripped event %s: %w", ev.ID, err)
	}
	ceMsg := msg.Copy()
	ceMsg.SetBytes(strippedBytes)
	ceMsg.MetaSetMut(rawparquet.MetaDataIndexKey, key)

	ct := ev.DataContentType
	if ct == "" {
		ct = contentTypeOctetStream
	}
	blobMsg := service.NewMessage(raw)
	blobMsg.MetaSetMut(rawparquet.MetaMessageContent, MetaBlobMessageContent)
	blobMsg.MetaSetMut(rawparquet.MetaS3UploadKey, key)
	blobMsg.MetaSetMut(rawparquet.MetaS3ContentType, ct)

	p.externalized.Incr(1)
	p.externalizedSize.Incr(int64(len(raw)))

	return service.MessageBatch{ceMsg, blobMsg}, nil
}

func dataPayloadLen(ev *cloudevent.RawEvent) int {
	if ev.DataBase64 != "" {
		return base64.StdEncoding.DecodedLen(len(ev.DataBase64))
	}
	return len(ev.Data)
}

func decodePayload(ev *cloudevent.RawEvent) ([]byte, error) {
	if ev.DataBase64 != "" {
		return base64.StdEncoding.DecodeString(ev.DataBase64)
	}
	return []byte(ev.Data), nil
}

func buildBlobKey(prefix, subject string, t time.Time) string {
	return fmt.Sprintf("%s%s/%d/%02d/%02d/%s",
		prefix, subject, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}
