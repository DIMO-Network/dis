package rawdocument

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors/rawparquet"
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

func newTestProcessor() *processor {
	res := service.MockResources()
	m := res.Metrics()
	return &processor{
		prefix:       "cloudevent/blobs/",
		logger:       res.Logger(),
		uploads:      m.NewCounter("uploads"),
		uploadBytes:  m.NewCounter("bytes"),
		uploadErrors: m.NewCounter("errors"),
	}
}

func TestProcessBatch_ProducesS3AndCHPairs(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "id-1", "did:erc721:1:0xV:1"),
		makeRawEventMsg(t, "id-2", "did:erc721:1:0xV:2"),
		makeRawEventMsg(t, "id-3", "did:erc721:1:0xV:3"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1, "should return exactly one batch")

	batch := result[0]
	require.Len(t, batch, 6, "should be 3 S3 + 3 CH rows")

	// Each pair: S3 message then CH message.
	for i := range 3 {
		s3Msg := batch[i*2]
		chMsg := batch[i*2+1]

		// S3 message has upload key and JSON body.
		s3Key, exists := s3Msg.MetaGet(rawparquet.MetaS3UploadKey)
		assert.True(t, exists, "S3 msg %d should have upload key", i)
		assert.Contains(t, s3Key, "cloudevent/blobs/")
		assert.Contains(t, s3Key, "single-")
		assert.Contains(t, s3Key, ".json")

		s3Bytes, err := s3Msg.AsBytes()
		require.NoError(t, err)
		assert.True(t, len(s3Bytes) > 0, "S3 msg %d body should not be empty", i)

		// CH message has correct metadata and structured row.
		content, exists := chMsg.MetaGet(rawparquet.MetaMessageContent)
		assert.True(t, exists, "CH msg %d should have content metadata", i)
		assert.Equal(t, rawparquet.MetaClickHouseCloudEvent, content)

		val, err := chMsg.AsStructured()
		require.NoError(t, err)
		row, ok := val.([]any)
		assert.True(t, ok, "CH msg %d should be a []any slice", i)
		assert.True(t, len(row) > 0)

		// CH row's last element (key) should match S3 key.
		assert.Equal(t, s3Key, row[len(row)-1].(string))
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
	assert.Empty(t, result, "all-invalid batch should return empty")
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
	require.Len(t, batch, 4, "should be 2 S3 + 2 CH rows")
}

func TestProcessBatch_SingleMessage(t *testing.T) {
	t.Parallel()
	proc := newTestProcessor()

	msgs := service.MessageBatch{
		makeRawEventMsg(t, "solo", "did:erc721:1:0xV:99"),
	}

	result, err := proc.ProcessBatch(context.Background(), msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	batch := result[0]
	require.Len(t, batch, 2, "1 S3 + 1 CH row")

	s3Key, _ := batch[0].MetaGet(rawparquet.MetaS3UploadKey)
	assert.Contains(t, s3Key, "cloudevent/blobs/")
	assert.Contains(t, s3Key, "did:erc721:1:0xV:99")
}

func TestBuildObjectKey_Format(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	key := buildObjectKey("cloudevent/blobs/", "did:erc721:1:0xV:42", ts)

	assert.Contains(t, key, "cloudevent/blobs/did:erc721:1:0xV:42/2024/03/07/single-")
	assert.Contains(t, key, ".json")
}

func TestBuildObjectKey_SubjectInPath(t *testing.T) {
	t.Parallel()
	ts := time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC)
	key := buildObjectKey("docs/", "my-subject", ts)

	assert.Contains(t, key, "docs/my-subject/2025/12/25/single-")
}
