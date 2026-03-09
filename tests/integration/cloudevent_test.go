//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudEventParquet(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:777"

	payload := map[string]any{
		"id":             "test-ce-parquet-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 777,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals": []map[string]any{
				{
					"timestamp": "2024-04-18T17:20:26.633Z",
					"name":      "speed",
					"value":     55,
				},
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	assert.Equal(t, 200, resp.StatusCode)

	// Wait for parquet batch flush (5s period + buffer)
	time.Sleep(8 * time.Second)

	// Check MinIO for parquet files
	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	// Decode parquet files and look for our event
	var found bool
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				found = true
				assert.Equal(t, testSourceAddress, ev.Source)
				assert.Equal(t, "dimo.status", ev.Type)
				break
			}
		}
		if found {
			break
		}
	}
	assert.True(t, found, "expected CloudEvent not found in parquet file")
}
