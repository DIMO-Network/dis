//go:build integration

package integration

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCloudEventParquet(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")

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
	t.Logf("Input payload: %s", string(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode)

	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka signals topic — verify the signal CE was produced ──
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.Len(t, msgs, 1, "expected exactly 1 signal message")
	ce := parseSignalCE(t, msgs[0])
	require.Equal(t, subject, ce.Subject)
	require.Equal(t, "dimo.signals", ce.Type)
	require.Equal(t, testSourceAddress, ce.Source)
	require.Len(t, ce.Data.Signals, 1, "expected exactly 1 signal in Kafka CE")
	require.Equal(t, "speed", ce.Data.Signals[0].Name)
	require.InDelta(t, 55.0, ce.Data.Signals[0].ValueNumber, 0.01)

	// ── 2. ClickHouse — verify signal row was written ──────────────
	rows := querySignals(t, subject)
	require.Len(t, rows, 1, "expected exactly 1 signal row in ClickHouse")
	require.Equal(t, "speed", rows[0].Name)
	require.InDelta(t, 55.0, rows[0].ValueNumber, 0.01)
	require.Equal(t, testSourceAddress, rows[0].Source)

	// ── 3. MinIO parquet — verify CloudEvent was archived ──────────
	// Wait for parquet batch flush
	time.Sleep(750 * time.Millisecond)

	keys := listMinIOObjects(t, "cloudevent/valid/")
	require.NotEmpty(t, keys, "no parquet files found in MinIO")

	var pqFound bool
	for _, key := range keys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				pqFound = true
				require.Equal(t, testSourceAddress, ev.Source)
				require.Equal(t, "dimo.status", ev.Type)
				require.Equal(t, "1.0", ev.SpecVersion)
				require.Equal(t, subject, ev.Producer)
				break
			}
		}
		if pqFound {
			break
		}
	}
	require.True(t, pqFound, "expected CloudEvent not found in parquet file")
}

func TestCloudEventDocument(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:9003"
	blobPrefix := "cloudevent/blobs/" + subject + "/"

	clearClickHouseForSubject(t, subject)
	clearMinIOObjects(t, blobPrefix)
	clearMinIOObjects(t, "cloudevent/valid/")

	payload := map[string]any{
		"id":             "test-ce-document-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:20:46.436008782Z",
		"vehicleTokenId": 9003,
		"data": map[string]any{
			"timestamp": 1713460846435,
			"signals": []map[string]any{
				{
					"timestamp": "2024-04-18T17:20:26.633Z",
					"name":      "speed",
					"value":     55,
				},
			},
			"padding": strings.Repeat("x", 2000),
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload size: %d bytes", len(payloadBytes))

	startOffset := kafkaEndOffset(t, "topic.device.signals")

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode)

	time.Sleep(750 * time.Millisecond)

	// ── 1. Kafka signals topic — verify the signal CE was still produced ──
	msgs := consumeKafka(t, "topic.device.signals", startOffset, 10*time.Second)
	require.NotEmpty(t, msgs, "expected at least 1 signal message on Kafka")
	ce := parseSignalCE(t, msgs[0])
	require.Equal(t, subject, ce.Subject)
	require.Equal(t, "dimo.signals", ce.Type)
	require.Equal(t, testSourceAddress, ce.Source)

	// ── 2. ClickHouse signals — verify signal rows were written ──
	rows := querySignals(t, subject)
	require.NotEmpty(t, rows, "expected signal rows in ClickHouse")
	var foundSpeed bool
	for _, r := range rows {
		if r.Name == "speed" {
			foundSpeed = true
			require.InDelta(t, 55.0, r.ValueNumber, 0.01)
		}
	}
	require.True(t, foundSpeed, "expected 'speed' signal in ClickHouse rows")

	// Wait for parquet batch flush
	time.Sleep(750 * time.Millisecond)

	// ── 3. ClickHouse cloud_event — index row references parquet AND blob ──
	ceRows := queryCloudEvents(t, subject)
	require.NotEmpty(t, ceRows, "expected cloud_event index rows in ClickHouse")
	var ceRow CloudEventRow
	for _, r := range ceRows {
		if r.EventType == "dimo.status" {
			ceRow = r
			break
		}
	}
	require.Equal(t, "dimo.status", ceRow.EventType, "no dimo.status cloud_event row found")
	require.True(t, strings.HasPrefix(ceRow.IndexKey, "cloudevent/valid/"),
		"expected index_key to point at parquet bundle, got %q", ceRow.IndexKey)
	require.True(t, strings.HasPrefix(ceRow.DataIndexKey, blobPrefix),
		"expected data_index_key to point at %s, got %q", blobPrefix, ceRow.DataIndexKey)
	require.Equal(t, testSourceAddress, ceRow.Source)

	// ── 4. MinIO data blob — contains just the data bytes, with content-type ──
	dataBytes, contentType := readBytesFromMinIO(t, ceRow.DataIndexKey)
	require.Equal(t, "application/json", contentType, "data blob should be served as application/json")
	var dataObj map[string]any
	require.NoError(t, json.Unmarshal(dataBytes, &dataObj),
		"data blob should be valid JSON: %s", string(dataBytes))
	require.Contains(t, dataObj, "padding", "data blob should be the inner data section, not the envelope")
	require.NotContains(t, dataObj, "specversion", "data blob should NOT contain envelope fields")

	// ── 5. MinIO parquet — row has empty data + DataIndexKey set ──
	// IndexKey is "<parquetKey>#<rowOffset>"; strip the offset.
	parquetKey := strings.SplitN(ceRow.IndexKey, "#", 2)[0]
	events := readParquetFromMinIO(t, parquetKey)
	var pqEvent *struct {
		Data         []byte
		DataIndexKey string
	}
	for _, ev := range events {
		if ev.Subject == subject && ev.Type == "dimo.status" {
			pqEvent = &struct {
				Data         []byte
				DataIndexKey string
			}{
				Data:         []byte(ev.Data),
				DataIndexKey: ev.DataIndexKey,
			}
			break
		}
	}
	require.NotNil(t, pqEvent, "expected dimo.status event in parquet bundle")
	require.Empty(t, pqEvent.Data, "parquet row Data should be empty when externalized")
	require.Equal(t, ceRow.DataIndexKey, pqEvent.DataIndexKey,
		"parquet row DataIndexKey should match cloud_event row data_index_key")
}

func TestCloudEventDocumentSmallPayload(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:9002"
	blobPrefix := "cloudevent/blobs/" + subject + "/"

	clearClickHouseForSubject(t, subject)
	clearMinIOObjects(t, blobPrefix)
	clearMinIOObjects(t, "cloudevent/valid/")

	payload := map[string]any{
		"id":             "test-ce-small-001",
		"source":         "will-be-overwritten",
		"dataschema":     "testschema/v2.0",
		"subject":        subject,
		"producer":       subject,
		"type":           "dimo.status",
		"time":           "2024-04-18T17:21:00.000000000Z",
		"vehicleTokenId": 9002,
		"data": map[string]any{
			"timestamp": 1713460860000,
			"signals": []map[string]any{
				{
					"timestamp": "2024-04-18T17:20:26.633Z",
					"name":      "speed",
					"value":     42,
				},
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	t.Logf("Input payload size: %d bytes (should be under 1024 threshold)", len(payloadBytes))

	resp := postMTLS(t, payloadBytes)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode)

	// Wait for batch flushes
	time.Sleep(1500 * time.Millisecond)

	// ── 1. Verify NO document blobs were created for this subject ──
	blobKeys := listMinIOObjects(t, blobPrefix)
	require.Empty(t, blobKeys, "small payload should not create document blobs, found: %v", blobKeys)

	// ── 2. Verify the event went to parquet path instead ──
	parquetKeys := listMinIOObjects(t, "cloudevent/valid/")
	var foundInParquet bool
	for _, key := range parquetKeys {
		events := readParquetFromMinIO(t, key)
		for _, ev := range events {
			if ev.Subject == subject {
				foundInParquet = true
				require.Equal(t, testSourceAddress, ev.Source)
				require.Equal(t, "dimo.status", ev.Type)
				break
			}
		}
		if foundInParquet {
			break
		}
	}
	require.True(t, foundInParquet, "small payload should appear in parquet, not document path")
}
