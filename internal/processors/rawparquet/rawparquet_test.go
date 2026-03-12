package rawparquet

import (
	"context"
	"encoding/json"
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

func TestProcessBatch_ProducesParquetPlusOriginals(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{prefix: "raw/test/", logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors")}

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
	proc := &processor{prefix: "raw/", logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors")}

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestProcessBatch_AllInvalid(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{prefix: "raw/", logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors")}

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
	proc := &processor{prefix: "raw/", logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors")}

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
	proc := &processor{prefix: "cloudevent/valid/", logger: res.Logger(), uploads: m.NewCounter("uploads"), uploadBytes: m.NewCounter("bytes"), uploadErrors: m.NewCounter("errors")}

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

func TestBuildObjectKey_Format(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	key := buildObjectKey("raw/data/", ts)

	assert.Contains(t, key, "raw/data/2024/03/07/batch-")
	assert.Contains(t, key, ".parquet")
}

func makeLargeRawEventMsg(t *testing.T, id, subject string, extraBytes int) *service.Message {
	t.Helper()
	padding := make([]byte, extraBytes)
	for i := range padding {
		padding[i] = 'x'
	}
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          id,
			Source:      "0xSource",
			Subject:     subject,
			Producer:    "did:nft:137:0xProd:1",
			Time:        time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:        "dimo.attestation",
		},
		Data: json.RawMessage(`"` + string(padding) + `"`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return service.NewMessage(b)
}

func TestProcessBatch_LargeEventBypassesParquet(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	const threshold = 200
	proc := &processor{
		prefix:              "cloudevent/valid/",
		largeEventThreshold: threshold,
		logger:              res.Logger(),
		uploads:             m.NewCounter("uploads"),
		uploadBytes:         m.NewCounter("bytes"),
		uploadErrors:        m.NewCounter("errors"),
	}

	smallMsg := makeRawEventMsg(t, "small-1", "did:erc721:1:0xV:1")
	largeMsg := makeLargeRawEventMsg(t, "large-1", "did:erc721:1:0xV:2", threshold+100)

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{smallMsg, largeMsg})
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	// 1 parquet + 1 CH row (small) + 1 S3 msg + 1 CH row (large) = 4
	require.Len(t, batch, 4)

	// First message is the parquet for the small event.
	parquetKey, exists := batch[0].MetaGet(MetaS3UploadKey)
	assert.True(t, exists)
	assert.Contains(t, parquetKey, ".parquet")
	assert.Equal(t, "1", mustMeta(t, batch[0], MetaParquetCount))

	// Second message is the CH row for the small event.
	assert.Equal(t, MetaClickHouseCloudEvent, mustMeta(t, batch[1], MetaMessageContent))

	// Third message is the direct S3 message for the large event.
	largeKey, exists := batch[2].MetaGet(MetaLargeObjectKey)
	assert.True(t, exists)
	assert.Contains(t, largeKey, "single-")
	assert.Contains(t, largeKey, ".json")
	assert.NotContains(t, largeKey, ".parquet")
	s3Key, exists := batch[2].MetaGet(MetaS3UploadKey)
	assert.True(t, exists)
	assert.Equal(t, largeKey, s3Key)
	assert.Equal(t, "application/json", mustMeta(t, batch[2], MetaS3ContentType))

	// Fourth message is the CH row for the large event.
	assert.Equal(t, MetaClickHouseCloudEvent, mustMeta(t, batch[3], MetaMessageContent))
}

func TestProcessBatch_AllLarge(t *testing.T) {
	t.Parallel()
	res := service.MockResources()
	m := res.Metrics()
	const threshold = 100
	proc := &processor{
		prefix:              "cloudevent/valid/",
		largeEventThreshold: threshold,
		logger:              res.Logger(),
		uploads:             m.NewCounter("uploads"),
		uploadBytes:         m.NewCounter("bytes"),
		uploadErrors:        m.NewCounter("errors"),
	}

	msgs := service.MessageBatch{
		makeLargeRawEventMsg(t, "large-1", "did:erc721:1:0xV:1", threshold+50),
		makeLargeRawEventMsg(t, "large-2", "did:erc721:1:0xV:2", threshold+50),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	// 2 large events × (1 S3 msg + 1 CH row) = 4 messages, no parquet
	require.Len(t, batch, 4)

	for _, msg := range batch {
		// No message should have MetaParquetPath (that's only on parquet messages)
		_, hasParquetPath := msg.MetaGet(MetaParquetPath)
		assert.False(t, hasParquetPath, "no parquet message should be emitted when all events are large")
	}
}

func TestBuildSingleObjectKey_Format(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	key := buildSingleObjectKey("cloudevent/valid/", ts)

	assert.Contains(t, key, "cloudevent/valid/2024/03/07/single-")
	assert.Contains(t, key, ".json")
}

func mustMeta(t *testing.T, msg *service.Message, key string) string {
	t.Helper()
	val, exists := msg.MetaGet(key)
	require.True(t, exists, "metadata key %q not found", key)
	return val
}
