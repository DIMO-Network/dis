package largecloudeventarchive

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

func makeLargeRawEventMsg(t *testing.T, id, subject string) *service.Message {
	t.Helper()

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
		Data: json.RawMessage(`{"payload":"large"}`),
	}

	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return service.NewMessage(b)
}

func TestProcessBatch_ProducesDirectObjectAndIndexRow(t *testing.T) {
	t.Parallel()

	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{
		prefix:       "cloudevent/valid/",
		logger:       res.Logger(),
		uploads:      m.NewCounter("uploads"),
		uploadBytes:  m.NewCounter("bytes"),
		uploadErrors: m.NewCounter("errors"),
		nowFn: func() time.Time {
			return time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC)
		},
	}

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{
		makeLargeRawEventMsg(t, "id-1", "did:erc721:1:0xV:1"),
	})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0], 2)

	s3Key, ok := result[0][0].MetaGet(MetaS3UploadKey)
	require.True(t, ok)
	assert.True(t, strings.HasPrefix(s3Key, "cloudevent/valid/2026/03/13/single-"))
	assert.True(t, strings.HasSuffix(s3Key, ".json"))

	contentType, ok := result[0][0].MetaGet(MetaS3ContentType)
	require.True(t, ok)
	assert.Equal(t, DefaultContentType, contentType)

	largeKey, ok := result[0][0].MetaGet(MetaLargeObjectKey)
	require.True(t, ok)
	assert.Equal(t, s3Key, largeKey)

	val, err := result[0][1].AsStructured()
	require.NoError(t, err)
	row := val.([]any)
	assert.Equal(t, s3Key, row[len(row)-1])

	content, ok := result[0][1].MetaGet(MetaMessageContent)
	require.True(t, ok)
	assert.Equal(t, MetaClickHouseCloudEvent, content)
}

func TestProcessBatch_SkipsInvalidMessages(t *testing.T) {
	t.Parallel()

	res := service.MockResources()
	m := res.Metrics()
	proc := &processor{
		prefix:       "cloudevent/valid/",
		logger:       res.Logger(),
		uploads:      m.NewCounter("uploads"),
		uploadBytes:  m.NewCounter("bytes"),
		uploadErrors: m.NewCounter("errors"),
		nowFn:        func() time.Time { return time.Now().UTC() },
	}

	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("not json")),
	})
	require.NoError(t, err)
	assert.Empty(t, result)
}
