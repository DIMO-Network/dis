package cloudeventsplit

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors/rawparquet"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProcessor(threshold int) *processor {
	res := service.MockResources()
	m := res.Metrics()
	return &processor{
		prefix:           "cloudevent/blobs/",
		threshold:        threshold,
		logger:           res.Logger(),
		externalized:     m.NewCounter("ext"),
		externalizedSize: m.NewCounter("extbytes"),
	}
}

func makeJSONEventMsg(t *testing.T, id, subject string, payloadSize int) *service.Message {
	t.Helper()
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion:     "1.0",
			ID:              id,
			Source:          "0xSource",
			Subject:         subject,
			Producer:        "did:nft:137:0xProd:1",
			Time:            time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:            "dimo.attestation",
			DataContentType: "application/json",
		},
		Data: json.RawMessage(`{"padding":"` + strings.Repeat("x", payloadSize) + `"}`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	msg := service.NewMessage(b)
	msg.MetaSetMut(rawparquet.MetaMessageContent, "dimo_valid_cloudevent")
	return msg
}

func makeBase64EventMsg(t *testing.T, id, subject, contentType string, raw []byte) *service.Message {
	t.Helper()
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion:     "1.0",
			ID:              id,
			Source:          "0xSource",
			Subject:         subject,
			Producer:        "did:nft:137:0xProd:1",
			Time:            time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:            "dimo.attestation",
			DataContentType: contentType,
		},
		DataBase64: base64.StdEncoding.EncodeToString(raw),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	msg := service.NewMessage(b)
	msg.MetaSetMut(rawparquet.MetaMessageContent, "dimo_valid_cloudevent")
	return msg
}

func TestProcessBatch_SmallEventPassesThrough(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor(1024)

	msg := makeJSONEventMsg(t, "small-1", "did:erc721:1:0xV:1", 64)
	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0], 1, "small event should pass through unchanged")

	_, hasIndexKey := result[0][0].MetaGet(rawparquet.MetaDataIndexKey)
	assert.False(t, hasIndexKey, "small event should not get data_index_key")
}

func TestProcessBatch_BigJSONEventSplits(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor(1024)

	subject := "did:erc721:1:0xV:99"
	msg := makeJSONEventMsg(t, "big-1", subject, 4096)
	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, result, 1)
	batch := result[0]
	require.Len(t, batch, 2, "big event should produce stripped CE + blob")

	// First message: stripped CE bound for the parquet stream.
	stripped := batch[0]
	dataKey, ok := stripped.MetaGet(rawparquet.MetaDataIndexKey)
	require.True(t, ok)
	assert.Contains(t, dataKey, "cloudevent/blobs/"+subject+"/")

	mc, _ := stripped.MetaGet(rawparquet.MetaMessageContent)
	assert.Equal(t, "dimo_valid_cloudevent", mc, "stripped CE keeps its content type")

	strippedBytes, err := stripped.AsBytes()
	require.NoError(t, err)
	var ev cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(strippedBytes, &ev))
	assert.Empty(t, ev.Data, "stripped CE should have empty data")
	assert.Empty(t, ev.DataBase64)
	assert.Equal(t, "big-1", ev.ID, "header should be preserved")

	// Second message: raw blob bound for the blob stream.
	blob := batch[1]
	blobMC, _ := blob.MetaGet(rawparquet.MetaMessageContent)
	assert.Equal(t, MetaBlobMessageContent, blobMC)

	blobKey, _ := blob.MetaGet(rawparquet.MetaS3UploadKey)
	assert.Equal(t, dataKey, blobKey, "stripped CE and blob must share a key")

	blobCT, _ := blob.MetaGet(rawparquet.MetaS3ContentType)
	assert.Equal(t, "application/json", blobCT)

	blobBytes, err := blob.AsBytes()
	require.NoError(t, err)
	var dataObj map[string]any
	require.NoError(t, json.Unmarshal(blobBytes, &dataObj))
	assert.Contains(t, dataObj, "padding")
}

func TestProcessBatch_BigBase64EventSplitsAndDecodes(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor(1024)

	raw := bytes.Repeat([]byte{0x00, 0x01, 0x7f, 0x80, 0xff}, 1024)
	subject := "did:erc721:1:0xV:99"
	msg := makeBase64EventMsg(t, "img-1", subject, "image/jpeg", raw)

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 2)

	blob := batch[1]
	gotBytes, err := blob.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, raw, gotBytes, "blob payload should be decoded raw bytes")

	ct, _ := blob.MetaGet(rawparquet.MetaS3ContentType)
	assert.Equal(t, "image/jpeg", ct)
}

func TestProcessBatch_MissingContentTypeFallsBack(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor(1024)

	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          "no-ct",
			Source:      "0xSource",
			Subject:     "did:erc721:1:0xV:1",
			Producer:    "did:nft:137:0xProd:1",
			Time:        time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:        "dimo.attestation",
		},
		Data: json.RawMessage(`"` + strings.Repeat("x", 4096) + `"`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	msg := service.NewMessage(b)

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 2)

	ct, _ := batch[1].MetaGet(rawparquet.MetaS3ContentType)
	assert.Equal(t, "application/octet-stream", ct)
}

func TestProcessBatch_ThresholdIsDataOnly(t *testing.T) {
	t.Parallel()
	// Threshold 1024 bytes against the data field. A header that pushes the
	// whole message over 1024 should NOT trigger a split if the data itself is
	// small.
	proc := newTestProcessor(1024)

	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          "small-data",
			Source:      "0xSource",
			Subject:     "did:erc721:1:0xV:1",
			Producer:    "did:nft:137:0xProd:1",
			Time:        time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:        "dimo.attestation",
			Tags:        []string{strings.Repeat("a", 800), strings.Repeat("b", 800)},
		},
		Data: json.RawMessage(`{"k":"v"}`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	require.Greater(t, len(b), 1024, "test setup: full message must exceed threshold")

	msg := service.NewMessage(b)
	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, result[0], 1, "small data should not split even when header is big")
}

func TestProcessBatch_BatchOfMixedSizes(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor(1024)

	msgs := service.MessageBatch{
		makeJSONEventMsg(t, "small", "did:erc721:1:0xV:1", 64),
		makeJSONEventMsg(t, "big", "did:erc721:1:0xV:2", 4096),
	}
	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 3, "1 small + 1 big (split into 2) = 3 messages")
}

func TestProcessBatch_MalformedPassesThrough(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor(1024)

	msg := service.NewMessage([]byte(`not json`))
	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, result[0], 1, "malformed message should pass through")
}
