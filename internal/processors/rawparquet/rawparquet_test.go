package rawparquet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRawEventMsg(t *testing.T, id, subject string) *service.Message {
	t.Helper()
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          id,
			Source:      "0xSource",
			Subject:     subject,
			Producer:    "did:nft:137:0xProd:1",
			Time:        time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:        "dimo.status",
		},
		Data: json.RawMessage(`{"speed": 55}`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return service.NewMessage(b)
}

func makeLargeRawEventMsg(t *testing.T, id, subject string, minSize int) *service.Message {
	t.Helper()
	paddingLen := minSize
	if paddingLen < 1 {
		paddingLen = 1
	}
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
		Data: json.RawMessage(`{"blob":"` + strings.Repeat("x", paddingLen) + `"}`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), minSize)
	return service.NewMessage(b)
}

func newTestProcessor(prefix string, largeEventThreshold int) *processor {
	res := service.MockResources()
	m := res.Metrics()
	return &processor{
		prefix:              prefix,
		largeEventThreshold: largeEventThreshold,
		logger:              res.Logger(),
		uploads:             m.NewCounter("uploads"),
		uploadBytes:         m.NewCounter("bytes"),
		uploadErrors:        m.NewCounter("errors"),
	}
}

func TestProcessBatch_ProducesParquetPlusOriginals(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor("raw/test/", 0)

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

	contentType, exists := parquetMsg.MetaGet(MetaS3ContentType)
	assert.True(t, exists)
	assert.Equal(t, "application/octet-stream", contentType)

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
	proc := newTestProcessor("raw/", 0)

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestProcessBatch_AllInvalid(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor("raw/", 0)

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
	proc := newTestProcessor("raw/", 0)

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
	proc := newTestProcessor("cloudevent/valid/", 0)

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

func TestBuildBatchObjectKey_Format(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	key := buildBatchObjectKey("raw/data/", ts)

	assert.Contains(t, key, "raw/data/2024/03/07/batch-")
	assert.Contains(t, key, ".parquet")
}

func TestProcessBatch_LargeEventsOnly(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor("cloudevent/valid/", 256)

	msgs := service.MessageBatch{
		makeLargeRawEventMsg(t, "large-1", "did:erc721:1:0xV:1", 256),
		makeLargeRawEventMsg(t, "large-2", "did:erc721:1:0xV:2", 256),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 4, "2 json uploads + 2 CH rows")

	for i := 0; i < len(batch); i += 2 {
		s3Key, exists := batch[i].MetaGet(MetaS3UploadKey)
		require.True(t, exists)
		assert.Contains(t, s3Key, "single-")
		assert.Contains(t, s3Key, ".json")

		contentType, exists := batch[i].MetaGet(MetaS3ContentType)
		require.True(t, exists)
		assert.Equal(t, "application/json", contentType)

		rowVal, err := batch[i+1].AsStructured()
		require.NoError(t, err)
		row, ok := rowVal.([]any)
		require.True(t, ok)
		require.Len(t, row, 10)
		indexKey, ok := row[9].(string)
		require.True(t, ok)
		assert.Equal(t, s3Key, indexKey)
		assert.NotContains(t, indexKey, "#")
	}
}

func TestProcessBatch_MixedSmallAndLarge(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor("cloudevent/valid/", 256)

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "small-1", "did:erc721:1:0xV:1"),
		makeLargeRawEventMsg(t, "large-1", "did:erc721:1:0xV:2", 256),
		makeRawEventMsg(t, "small-2", "did:erc721:1:0xV:3"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 5, "1 parquet + 2 parquet CH rows + 1 json upload + 1 json CH row")

	parquetKey, exists := batch[0].MetaGet(MetaS3UploadKey)
	require.True(t, exists)
	assert.Contains(t, parquetKey, ".parquet")
	contentType, exists := batch[0].MetaGet(MetaS3ContentType)
	require.True(t, exists)
	assert.Equal(t, "application/octet-stream", contentType)
	parquetCount, exists := batch[0].MetaGet(MetaParquetCount)
	require.True(t, exists)
	assert.Equal(t, "2", parquetCount)

	for i := 1; i <= 2; i++ {
		rowVal, err := batch[i].AsStructured()
		require.NoError(t, err)
		row, ok := rowVal.([]any)
		require.True(t, ok)
		indexKey, ok := row[9].(string)
		require.True(t, ok)
		assert.Contains(t, indexKey, "#")
	}

	s3Key, exists := batch[3].MetaGet(MetaS3UploadKey)
	require.True(t, exists)
	assert.Contains(t, s3Key, "single-")
	assert.Contains(t, s3Key, ".json")
	contentType, exists = batch[3].MetaGet(MetaS3ContentType)
	require.True(t, exists)
	assert.Equal(t, "application/json", contentType)

	rowVal, err := batch[4].AsStructured()
	require.NoError(t, err)
	row, ok := rowVal.([]any)
	require.True(t, ok)
	indexKey, ok := row[9].(string)
	require.True(t, ok)
	assert.Equal(t, s3Key, indexKey)
	assert.NotContains(t, indexKey, "#")
}

func TestProcessBatch_ThresholdDisabled(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor("cloudevent/valid/", 0)

	msgs := service.MessageBatch{
		makeLargeRawEventMsg(t, "large-1", "did:erc721:1:0xV:1", 256),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0], 2, "1 parquet + 1 CH row")

	s3Key, exists := result[0][0].MetaGet(MetaS3UploadKey)
	require.True(t, exists)
	assert.Contains(t, s3Key, ".parquet")
	assert.NotContains(t, s3Key, "single-")

	rowVal, err := result[0][1].AsStructured()
	require.NoError(t, err)
	row, ok := rowVal.([]any)
	require.True(t, ok)
	indexKey, ok := row[9].(string)
	require.True(t, ok)
	assert.Contains(t, indexKey, "#")
}

func TestBuildSingleObjectKey_Format(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	key := buildSingleObjectKey("raw/data/", ts)

	assert.Contains(t, key, "raw/data/2024/03/07/single-")
	assert.Contains(t, key, ".json")
}
