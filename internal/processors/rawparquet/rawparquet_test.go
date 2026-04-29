package rawparquet

import (
	"bytes"
	"context"
	"encoding/json"
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

func newTestProcessor() *processor {
	res := service.MockResources()
	m := res.Metrics()
	return &processor{
		prefix:       "raw/test/",
		logger:       res.Logger(),
		uploads:      m.NewCounter("uploads"),
		uploadBytes:  m.NewCounter("bytes"),
		uploadErrors: m.NewCounter("errors"),
	}
}

// Column order matches cloudevent/clickhouse.InsertStmt:
// 0:subject, 1:time, 2:type, 3:id, 4:source, 5:producer,
// 6:data_content_type, 7:data_version, 8:extras,
// 9:index_key, 10:data_index_key, 11:voids_id.
const (
	chColIndexKey     = 9
	chColDataIndexKey = 10
	chColVoidsID      = 11
)

// chRowKey returns the index_key column from a ClickHouse row message.
func chRowKey(t *testing.T, msg *service.Message) string {
	t.Helper()
	val, err := msg.AsStructured()
	require.NoError(t, err)
	row := val.([]any)
	return row[chColIndexKey].(string)
}

// chRowDataKey returns the data_index_key column from a ClickHouse row message.
func chRowDataKey(t *testing.T, msg *service.Message) string {
	t.Helper()
	val, err := msg.AsStructured()
	require.NoError(t, err)
	row := val.([]any)
	return row[chColDataIndexKey].(string)
}

// chRowVoidsID returns the voids_id column from a ClickHouse row message.
func chRowVoidsID(t *testing.T, msg *service.Message) string {
	t.Helper()
	val, err := msg.AsStructured()
	require.NoError(t, err)
	row := val.([]any)
	return row[chColVoidsID].(string)
}

// chRowType returns the type column from a ClickHouse row message. Column
// order is subject, time, type, ... so type is at index 2.
func chRowType(t *testing.T, msg *service.Message) string {
	t.Helper()
	val, err := msg.AsStructured()
	require.NoError(t, err)
	row := val.([]any)
	return row[2].(string)
}

func TestProcessBatch_ProducesParquetPlusOriginals(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "id-1", "did:erc721:1:0xV:1"),
		makeRawEventMsg(t, "id-2", "did:erc721:1:0xV:2"),
		makeRawEventMsg(t, "id-3", "did:erc721:1:0xV:3"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 4, "should be 1 parquet + 3 CH rows")

	parquetMsg := batch[0]
	parquetBytes, err := parquetMsg.AsBytes()
	require.NoError(t, err)
	assert.True(t, len(parquetBytes) > 0)

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

	for i := 1; i < len(batch); i++ {
		content, exists := batch[i].MetaGet(MetaMessageContent)
		assert.True(t, exists, "CH row message %d should have content metadata", i)
		assert.Equal(t, MetaClickHouseCloudEvent, content)
		val, err := batch[i].AsStructured()
		assert.NoError(t, err)
		row, ok := val.([]any)
		assert.True(t, ok)
		assert.True(t, len(row) > 0)
	}
}

func TestProcessBatch_Empty(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestProcessBatch_AllInvalid(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		service.NewMessage([]byte(`not json`)),
		service.NewMessage([]byte(`{also broken`)),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestProcessBatch_MixedValidInvalid(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "good-1", "did:erc721:1:0xV:1"),
		service.NewMessage([]byte(`not json`)),
		makeRawEventMsg(t, "good-2", "did:erc721:1:0xV:2"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 3)

	parquetCount, _ := batch[0].MetaGet(MetaParquetCount)
	assert.Equal(t, "2", parquetCount)
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
	require.Len(t, batch, 3)

	parquetCount, _ := batch[0].MetaGet(MetaParquetCount)
	assert.Equal(t, "1", parquetCount)

	key1 := chRowKey(t, batch[1])
	key2 := chRowKey(t, batch[2])
	assert.Equal(t, key1, key2)
}

func TestProcessBatch_DedupPreservesOriginalTypeInCHRows(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	// Status and fingerprint share an ID. Dedup should collapse parquet to one
	// row, but each CH row must report its own original type — otherwise the
	// CH ReplacingMergeTree merges two identical rows and the fingerprint
	// disappears.
	msgs := service.MessageBatch{
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.status"),
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.fingerprint"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 3)

	t1 := chRowType(t, batch[1])
	t2 := chRowType(t, batch[2])
	assert.ElementsMatch(t, []string{"dimo.status", "dimo.fingerprint"}, []string{t1, t2},
		"each CH row should keep its original type")
}

func TestProcessBatch_DeduplicatesPrefersStatus(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.fingerprint"),
		makeRawEventMsgWithType(t, "shared-id", "did:erc721:1:0xV:1", "dimo.status"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 3)

	parquetCount, _ := batch[0].MetaGet(MetaParquetCount)
	assert.Equal(t, "1", parquetCount)

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
	batch := result[0]
	require.Len(t, batch, 3)

	parquetCount, _ := batch[0].MetaGet(MetaParquetCount)
	assert.Equal(t, "2", parquetCount)

	assert.NotEqual(t, chRowKey(t, batch[1]), chRowKey(t, batch[2]))
}

func TestProcessBatch_DataIndexKeyFromMetadata(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	subject := "did:erc721:1:0xV:1"
	msg := makeRawEventMsg(t, "ext-id", subject)
	msg.MetaSetMut(MetaDataIndexKey, "cloudevent/blobs/some/key")

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 2)

	// CH row's data_index_key column should match the metadata value.
	assert.Equal(t, "cloudevent/blobs/some/key", chRowDataKey(t, batch[1]))

	// Parquet row should carry the same DataIndexKey.
	parquetBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	events, err := pq.Decode(bytes.NewReader(parquetBytes), int64(len(parquetBytes)))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "cloudevent/blobs/some/key", events[0].DataIndexKey)
}

func TestProcessBatch_VoidsIDFromMetadata(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	subject := "did:erc721:1:0xV:1"
	msg := makeRawEventMsgWithType(t, "tombstone-row-1", subject, "dimo.tombstone")
	msg.MetaSetMut(MetaVoidsID, "target-attestation-id-1")

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{msg})
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 2)

	// The CH row should carry the voids_id from metadata, with data_index_key empty.
	assert.Equal(t, "target-attestation-id-1", chRowVoidsID(t, batch[1]))
	assert.Equal(t, "", chRowDataKey(t, batch[1]))
}

func TestProcessBatch_NoVoidsIDMetaIsEmpty(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "regular-event", "did:erc721:1:0xV:1"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 2)

	assert.Equal(t, "", chRowVoidsID(t, batch[1]))
}

func TestProcessBatch_NoDataIndexKeyMetaIsEmpty(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "inline", "did:erc721:1:0xV:1"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	batch := result[0]
	require.Len(t, batch, 2)

	assert.Equal(t, "", chRowDataKey(t, batch[1]))

	parquetBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	events, err := pq.Decode(bytes.NewReader(parquetBytes), int64(len(parquetBytes)))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "", events[0].DataIndexKey)
	assert.NotEmpty(t, events[0].Data, "inline event should retain its data")
}

func TestBuildObjectKey_Format(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	key := buildObjectKey("raw/data/", ts)

	assert.Contains(t, key, "raw/data/2024/03/07/batch-")
	assert.Contains(t, key, ".parquet")
}
