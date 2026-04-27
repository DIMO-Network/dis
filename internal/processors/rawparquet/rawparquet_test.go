package rawparquet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	pq "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRawEventMsg(t *testing.T, id, subject string) *service.Message {
	t.Helper()
	return makeRawEventMsgWithType(t, id, subject, "dimo.status")
}

func makeRawEventMsgWithType(t *testing.T, id, subject, eventType string) *service.Message {
	t.Helper()
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          id,
			Source:      "0xSource",
			Subject:     subject,
			Producer:    "did:nft:137:0xProd:1",
			Time:        time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:        eventType,
		},
		Data: json.RawMessage(`{"speed": 55}`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return service.NewMessage(b)
}

func TestProcessBatch_ProducesParquetPlusOriginals(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{prefix: "raw/test/", dataPrefix: "raw/blobs/", dataSizeThresh: 1 << 20, logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors"), dataExternals: m.NewCounter("externals"), dataExternalSize: m.NewCounter("externalbytes")}

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "id-1", "did:erc721:1:0xV:1"),
		makeRawEventMsg(t, "id-2", "did:erc721:1:0xV:2"),
		makeRawEventMsg(t, "id-3", "did:erc721:1:0xV:3"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1, "should return exactly one batch")

	batch := result[0]
	require.Len(t, batch, 4, "should be 1 parquet + 3 CH rows")

	// First message is the parquet file.
	parquetMsg := batch[0]
	parquetBytes, err := parquetMsg.AsBytes()
	require.NoError(t, err)
	assert.True(t, len(parquetBytes) > 0, "parquet file should not be empty")

	// Check parquet metadata.
	s3Key, exists := parquetMsg.MetaGet(MetaS3UploadKey)
	assert.True(t, exists)
	assert.Contains(t, s3Key, "raw/test/")
	assert.Contains(t, s3Key, ".parquet")

	parquetPath, exists := parquetMsg.MetaGet(MetaParquetPath)
	assert.True(t, exists)
	assert.Equal(t, s3Key, parquetPath)

	parquetSize, exists := parquetMsg.MetaGet(MetaParquetSize)
	assert.True(t, exists)
	assert.NotEqual(t, "0", parquetSize)

	parquetCount, exists := parquetMsg.MetaGet(MetaParquetCount)
	assert.True(t, exists)
	assert.Equal(t, "3", parquetCount)

	// Remaining messages should be ClickHouse row messages.
	for i := 1; i < len(batch); i++ {
		content, exists := batch[i].MetaGet(MetaMessageContent)
		assert.True(t, exists, "CH row message %d should have content metadata", i)
		assert.Equal(t, MetaClickHouseCloudEvent, content, "CH row message %d should have correct content type", i)
		val, err := batch[i].AsStructured()
		assert.NoError(t, err, "CH row message %d should have structured content", i)
		row, ok := val.([]any)
		assert.True(t, ok, "CH row message %d should be a []any slice", i)
		assert.True(t, len(row) > 0, "CH row message %d should not be empty", i)
	}
}

func TestProcessBatch_Empty(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{prefix: "raw/", dataPrefix: "raw/blobs/", dataSizeThresh: 1 << 20, logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors"), dataExternals: m.NewCounter("externals"), dataExternalSize: m.NewCounter("externalbytes")}

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestProcessBatch_AllInvalid(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{prefix: "raw/", dataPrefix: "raw/blobs/", dataSizeThresh: 1 << 20, logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors"), dataExternals: m.NewCounter("externals"), dataExternalSize: m.NewCounter("externalbytes")}

	msgs := service.MessageBatch{
		service.NewMessage([]byte(`not json`)),
		service.NewMessage([]byte(`{also broken`)),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	assert.Empty(t, result, "all-invalid batch should return empty")
}

func TestProcessBatch_MixedValidInvalid(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{prefix: "raw/", dataPrefix: "raw/blobs/", dataSizeThresh: 1 << 20, logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors"), dataExternals: m.NewCounter("externals"), dataExternalSize: m.NewCounter("externalbytes")}

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "good-1", "did:erc721:1:0xV:1"),
		service.NewMessage([]byte(`not json`)),
		makeRawEventMsg(t, "good-2", "did:erc721:1:0xV:2"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	// 1 parquet + 2 CH rows (invalid one is skipped).
	require.Len(t, batch, 3)

	parquetCount, _ := batch[0].MetaGet(MetaParquetCount)
	assert.Equal(t, "2", parquetCount)
}

func TestProcessBatch_SingleMessage(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{prefix: "cloudevent/valid/", dataPrefix: "cloudevent/blobs/", dataSizeThresh: 1 << 20, logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors"), dataExternals: m.NewCounter("externals"), dataExternalSize: m.NewCounter("externalbytes")}

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "solo", "did:erc721:1:0xV:99"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 2, "1 parquet + 1 CH row")

	s3Key, _ := batch[0].MetaGet(MetaS3UploadKey)
	assert.Contains(t, s3Key, "cloudevent/valid/")

	parquetCount, _ := batch[0].MetaGet(MetaParquetCount)
	assert.Equal(t, "1", parquetCount)
}

func newTestProcessor() *processor {
	res := service.MockResources()
	m := res.Metrics()
	return &processor{
		prefix:           "raw/test/",
		dataPrefix:       "raw/blobs/",
		dataSizeThresh:   1 << 20,
		logger:           res.Logger(),
		uploads:          m.NewCounter("uploads"),
		uploadBytes:      m.NewCounter("bytes"),
		uploadErrors:     m.NewCounter("errors"),
		dataExternals:    m.NewCounter("externals"),
		dataExternalSize: m.NewCounter("externalbytes"),
	}
}

// chRowKey returns the index_key column from a ClickHouse row message.
// Column order is ..., index_key, data_index_key — index_key is second-to-last.
func chRowKey(t *testing.T, msg *service.Message) string {
	t.Helper()
	val, err := msg.AsStructured()
	require.NoError(t, err)
	row := val.([]any)
	return row[len(row)-2].(string)
}

// chRowDataKey returns the data_index_key column from a ClickHouse row message.
func chRowDataKey(t *testing.T, msg *service.Message) string {
	t.Helper()
	val, err := msg.AsStructured()
	require.NoError(t, err)
	row := val.([]any)
	return row[len(row)-1].(string)
}

func TestProcessBatch_DeduplicatesSameID(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.status"),
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.fingerprint"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	// 1 parquet message + 2 ClickHouse rows.
	require.Len(t, batch, 3, "should be 1 parquet + 2 CH rows")

	parquetCount, exists := batch[0].MetaGet(MetaParquetCount)
	assert.True(t, exists)
	assert.Equal(t, "1", parquetCount, "only 1 unique event should be encoded in parquet")

	// Both CH rows should reference the same parquet key.
	key1 := chRowKey(t, batch[1])
	key2 := chRowKey(t, batch[2])
	assert.Equal(t, key1, key2, "duplicate-ID CH rows should reference the same parquet key")
}

func TestProcessBatch_DeduplicatesPrefersStatus(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	// Fingerprint arrives before status — parquet should still contain the status event.
	msgs := service.MessageBatch{
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.fingerprint"),
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.status"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 3, "should be 1 parquet + 2 CH rows")

	parquetCount, exists := batch[0].MetaGet(MetaParquetCount)
	assert.True(t, exists)
	assert.Equal(t, "1", parquetCount)

	// Both CH rows should reference the same parquet key.
	key1 := chRowKey(t, batch[1])
	key2 := chRowKey(t, batch[2])
	assert.Equal(t, key1, key2)
}

func TestProcessBatch_NoDedupDifferentIDs(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsgWithType(t, "id-a", "did:erc721:1:0xV:1", "dimo.status"),
		makeRawEventMsgWithType(t, "id-b", "did:erc721:1:0xV:1", "dimo.fingerprint"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 3, "should be 1 parquet + 2 CH rows")

	parquetCount, exists := batch[0].MetaGet(MetaParquetCount)
	assert.True(t, exists)
	assert.Equal(t, "2", parquetCount, "different IDs should not be deduplicated")

	key1 := chRowKey(t, batch[1])
	key2 := chRowKey(t, batch[2])
	assert.NotEqual(t, key1, key2, "different-ID CH rows should reference different parquet keys")
}

func TestProcessBatch_MixedDedupAndUnique(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsgWithType(t, "dup-id", "did:erc721:1:0xV:1", "dimo.status"),
		makeRawEventMsgWithType(t, "dup-id", "did:erc721:1:0xV:1", "dimo.fingerprint"),
		makeRawEventMsgWithType(t, "unique-id", "did:erc721:1:0xV:2", "dimo.status"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	// 1 parquet + 3 CH rows.
	require.Len(t, batch, 4, "should be 1 parquet + 3 CH rows")

	parquetCount, exists := batch[0].MetaGet(MetaParquetCount)
	assert.True(t, exists)
	assert.Equal(t, "2", parquetCount, "2 unique events (dup-id counted once + unique-id)")

	// The two dup-id CH rows should share a key.
	key1 := chRowKey(t, batch[1])
	key2 := chRowKey(t, batch[2])
	key3 := chRowKey(t, batch[3])
	assert.Equal(t, key1, key2, "duplicate-ID CH rows should share parquet key")
	assert.NotEqual(t, key1, key3, "unique-ID CH row should have a different parquet key")
}

func TestBuildObjectKey_Format(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	key := buildObjectKey("raw/data/", ts)

	assert.Contains(t, key, "raw/data/2024/03/07/batch-")
	assert.Contains(t, key, ".parquet")
}

// makeBigJSONEventMsg builds a CloudEvent whose JSON `data` field is at least
// payloadSize bytes — useful for triggering the externalization branch.
func makeBigJSONEventMsg(t *testing.T, id, subject string, payloadSize int) *service.Message {
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
	return service.NewMessage(b)
}

// makeBigBase64EventMsg builds a CloudEvent whose `data_base64` field decodes
// to the provided raw bytes, with the supplied datacontenttype.
func makeBigBase64EventMsg(t *testing.T, id, subject, contentType string, raw []byte) *service.Message {
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
	return service.NewMessage(b)
}

// findS3DataMessages returns batch messages tagged with an S3 upload key under
// the given data-blob prefix (i.e. per-event data PUTs, not the parquet bundle).
func findS3DataMessages(batch service.MessageBatch, dataPrefix string) []*service.Message {
	var out []*service.Message
	for _, m := range batch {
		key, ok := m.MetaGet(MetaS3UploadKey)
		if !ok {
			continue
		}
		if strings.HasPrefix(key, dataPrefix) {
			out = append(out, m)
		}
	}
	return out
}

func TestProcessBatch_SmallEventStaysInline(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "small-1", "did:erc721:1:0xV:1"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)
	batch := result[0]
	require.Len(t, batch, 2, "small event: 1 parquet + 1 CH row, no data blobs")

	dataMsgs := findS3DataMessages(batch, proc.dataPrefix)
	assert.Empty(t, dataMsgs, "small event should not emit a data blob message")

	assert.Equal(t, "", chRowDataKey(t, batch[1]), "data_index_key should be empty for inline data")

	parquetBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	events, err := pq.Decode(bytes.NewReader(parquetBytes), int64(len(parquetBytes)))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.NotEmpty(t, events[0].Data, "small event parquet row should keep inline data")
	assert.Equal(t, "", events[0].DataIndexKey)
}

func TestProcessBatch_BigJSONEventExternalized(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()
	proc.dataSizeThresh = 1024 // ~1 KiB so the test payload trips it

	subject := "did:erc721:1:0xV:99"
	msgs := service.MessageBatch{
		makeBigJSONEventMsg(t, "big-1", subject, 4096),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)
	batch := result[0]
	// 1 parquet + 1 data blob + 1 CH row.
	require.Len(t, batch, 3)

	dataMsgs := findS3DataMessages(batch, proc.dataPrefix)
	require.Len(t, dataMsgs, 1)
	dataMsg := dataMsgs[0]

	dataKey, _ := dataMsg.MetaGet(MetaS3UploadKey)
	assert.Contains(t, dataKey, "raw/blobs/"+subject+"/", "data blob key should include subject partition")
	ct, _ := dataMsg.MetaGet(MetaS3ContentType)
	assert.Equal(t, "application/json", ct, "JSON data should keep its application/json content-type")

	dataBytes, err := dataMsg.AsBytes()
	require.NoError(t, err)
	var dataObj map[string]any
	require.NoError(t, json.Unmarshal(dataBytes, &dataObj),
		"data blob should be valid JSON")
	assert.Contains(t, dataObj, "padding")

	// CH row: index_key is parquet#offset; data_index_key matches the blob.
	assert.Contains(t, chRowKey(t, batch[2]), ".parquet#")
	assert.Equal(t, dataKey, chRowDataKey(t, batch[2]))

	// Parquet row: data and data_base64 cleared, DataIndexKey populated.
	parquetBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	events, err := pq.Decode(bytes.NewReader(parquetBytes), int64(len(parquetBytes)))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Empty(t, events[0].Data, "externalized parquet row should have empty Data")
	assert.Equal(t, "", events[0].DataBase64)
	assert.Equal(t, dataKey, events[0].DataIndexKey)
}

func TestProcessBatch_BigBase64EventDecoded(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()
	proc.dataSizeThresh = 1024

	// Pretend this is a JPEG body. Use random-ish bytes so we can verify
	// the S3 object isn't accidentally re-encoded as base64.
	raw := bytes.Repeat([]byte{0x00, 0x01, 0x02, 0x03, 0x7f, 0x80, 0xff}, 1024)
	msgs := service.MessageBatch{
		makeBigBase64EventMsg(t, "img-1", "did:erc721:1:0xV:99", "image/jpeg", raw),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)
	batch := result[0]
	require.Len(t, batch, 3)

	dataMsgs := findS3DataMessages(batch, proc.dataPrefix)
	require.Len(t, dataMsgs, 1)
	dataMsg := dataMsgs[0]

	ct, _ := dataMsg.MetaGet(MetaS3ContentType)
	assert.Equal(t, "image/jpeg", ct)

	gotBytes, err := dataMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, raw, gotBytes, "S3 object should be the decoded raw bytes, not base64")

	dataKey, _ := dataMsg.MetaGet(MetaS3UploadKey)
	assert.Equal(t, dataKey, chRowDataKey(t, batch[2]))
}

func TestProcessBatch_MissingContentTypeFallsBack(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()
	proc.dataSizeThresh = 1024

	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          "no-ct",
			Source:      "0xSource",
			Subject:     "did:erc721:1:0xV:1",
			Producer:    "did:nft:137:0xProd:1",
			Time:        time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:        "dimo.attestation",
			// No DataContentType.
		},
		Data: json.RawMessage(`"` + strings.Repeat("x", 4096) + `"`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	msgs := service.MessageBatch{service.NewMessage(b)}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	batch := result[0]

	dataMsgs := findS3DataMessages(batch, proc.dataPrefix)
	require.Len(t, dataMsgs, 1)
	ct, _ := dataMsgs[0].MetaGet(MetaS3ContentType)
	assert.Equal(t, "application/octet-stream", ct, "missing datacontenttype should fall back to octet-stream")
}

func TestProcessBatch_DedupSharesDataIndexKey(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()
	proc.dataSizeThresh = 1024

	subject := "did:erc721:1:0xV:1"
	msgs := service.MessageBatch{
		makeBigJSONEventMsg(t, "shared-id", subject, 4096),
		makeBigJSONEventMsg(t, "shared-id", subject, 4096),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	batch := result[0]

	// Dedup keeps one parquet row, so we expect: 1 parquet + 1 data blob + 2 CH rows.
	require.Len(t, batch, 4)
	dataMsgs := findS3DataMessages(batch, proc.dataPrefix)
	require.Len(t, dataMsgs, 1, "duplicate IDs should produce a single externalized data PUT")

	chRowMsgs := batch[2:]
	require.Equal(t, chRowDataKey(t, chRowMsgs[0]), chRowDataKey(t, chRowMsgs[1]),
		"both CH rows for the same parquet entry should reference the same data_index_key")
}
