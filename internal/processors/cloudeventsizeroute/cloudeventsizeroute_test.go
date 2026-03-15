package cloudeventsizeroute

import (
	"context"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcess_SetsSizeAndRouteMetadata(t *testing.T) {
	t.Parallel()

	proc := &processor{
		threshold:        4,
		sizeMetadataKey:  defaultSizeMetadataKey,
		routeMetadataKey: defaultRouteMetadataKey,
	}

	msg := service.NewMessage([]byte("12345"))
	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	size, ok := batch[0].MetaGet(defaultSizeMetadataKey)
	require.True(t, ok)
	assert.Equal(t, "5", size)

	large, ok := batch[0].MetaGet(defaultRouteMetadataKey)
	require.True(t, ok)
	assert.Equal(t, "true", large)
}

func TestProcess_EqualThresholdRemainsSmall(t *testing.T) {
	t.Parallel()

	proc := &processor{
		threshold:        5,
		sizeMetadataKey:  defaultSizeMetadataKey,
		routeMetadataKey: defaultRouteMetadataKey,
	}

	msg := service.NewMessage([]byte("12345"))
	batch, err := proc.Process(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	large, ok := batch[0].MetaGet(defaultRouteMetadataKey)
	require.True(t, ok)
	assert.Equal(t, "false", large)
}
