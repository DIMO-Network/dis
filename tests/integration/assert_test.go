//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaEndOffset returns the current end offset (high-water mark) for a single-partition topic.
// Call this before sending messages, then pass the result to consumeKafka so the test
// only sees messages produced after the snapshot.
func kafkaEndOffset(t *testing.T, topic string) int64 {
	t.Helper()
	ctx := context.Background()
	offsets, err := kafkaAdminClient.ListEndOffsets(ctx, topic)
	require.NoError(t, err)

	o, ok := offsets.Lookup(topic, 0)
	require.True(t, ok, "topic %s partition 0 not found", topic)
	require.NoError(t, o.Err)
	return o.Offset
}

// consumeKafka consumes messages from `topic` starting at `fromOffset` within `timeout`.
// Returns the raw message bytes. Logs full payload of each message.
func consumeKafka(t *testing.T, topic string, fromOffset int64, timeout time.Duration) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(fromOffset)),
	)
	require.NoError(t, err)
	defer cl.Close()

	var msgs [][]byte
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded {
					t.Logf("Kafka topic %q: consumed %d messages (from offset %d)", topic, len(msgs), fromOffset)
					return msgs
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			// Pretty-print JSON payload
			var pretty bytes.Buffer
			if err := json.Indent(&pretty, r.Value, "  ", "  "); err == nil {
				t.Logf("Kafka %s [partition=%d offset=%d]:\n  %s", topic, r.Partition, r.Offset, pretty.String())
			} else {
				t.Logf("Kafka %s [partition=%d offset=%d]: %s", topic, r.Partition, r.Offset, string(r.Value))
			}
			msgs = append(msgs, r.Value)
		})
	}
}

// SignalRow represents a row from the ClickHouse signal table.
type SignalRow struct {
	Subject      string    `db:"subject"`
	Timestamp    time.Time `db:"timestamp"`
	Name         string    `db:"name"`
	Source       string    `db:"source"`
	Producer     string    `db:"producer"`
	CloudEventID string    `db:"cloud_event_id"`
	ValueNumber  float64   `db:"value_number"`
	ValueString  string    `db:"value_string"`
}

// clearClickHouseForSubject deletes all rows for a subject from signal, event, and cloud_event tables.
func clearClickHouseForSubject(t *testing.T, subject string) {
	t.Helper()
	_, err := clickhouseDB.Exec("ALTER TABLE dimo.signal DELETE WHERE subject = ?", subject)
	require.NoError(t, err)
	_, err = clickhouseDB.Exec("ALTER TABLE dimo.event DELETE WHERE subject = ?", subject)
	require.NoError(t, err)

	indexDB, err := sql.Open("clickhouse", clickhouseIndexDSN)
	require.NoError(t, err)
	defer indexDB.Close()
	_, err = indexDB.Exec("ALTER TABLE cloud_event DELETE WHERE subject = ?", subject)
	require.NoError(t, err)
}

// querySignals queries the ClickHouse signal table for a given subject.
// Logs full row data for each result.
func querySignals(t *testing.T, subject string) []SignalRow {
	t.Helper()
	// Force merge to ensure data is visible
	_, err := clickhouseDB.Exec("OPTIMIZE TABLE dimo.signal FINAL")
	require.NoError(t, err)

	rows, err := clickhouseDB.Query(
		"SELECT subject, timestamp, name, source, producer, cloud_event_id, value_number, value_string FROM dimo.signal WHERE subject = ? ORDER BY name",
		subject,
	)
	require.NoError(t, err)
	defer rows.Close()

	var result []SignalRow
	for rows.Next() {
		var r SignalRow
		err := rows.Scan(&r.Subject, &r.Timestamp, &r.Name, &r.Source, &r.Producer, &r.CloudEventID, &r.ValueNumber, &r.ValueString)
		require.NoError(t, err)
		result = append(result, r)
	}
	require.NoError(t, rows.Err())

	t.Logf("ClickHouse dimo.signal: %d rows for subject=%s", len(result), subject)
	for i, r := range result {
		t.Logf("  signal[%d]: subject=%s name=%s valueNumber=%v valueString=%q timestamp=%v source=%s producer=%s cloudEventID=%s",
			i, r.Subject, r.Name, r.ValueNumber, r.ValueString, r.Timestamp, r.Source, r.Producer, r.CloudEventID)
	}
	return result
}

// EventRow represents a row from the ClickHouse event table.
type EventRow struct {
	Subject      string    `db:"subject"`
	Source       string    `db:"source"`
	Producer     string    `db:"producer"`
	CloudEventID string    `db:"cloud_event_id"`
	Type         string    `db:"type"`
	DataVersion  string    `db:"data_version"`
	Name         string    `db:"name"`
	Timestamp    time.Time `db:"timestamp"`
	DurationNs   uint64    `db:"duration_ns"`
	Metadata     string    `db:"metadata"`
}

// queryEvents queries the ClickHouse event table for a given subject.
// Logs full row data for each result.
func queryEvents(t *testing.T, subject string) []EventRow {
	t.Helper()
	_, err := clickhouseDB.Exec("OPTIMIZE TABLE dimo.event FINAL")
	require.NoError(t, err)

	rows, err := clickhouseDB.Query(
		"SELECT subject, source, producer, cloud_event_id, type, data_version, name, timestamp, duration_ns, metadata FROM dimo.event WHERE subject = ? ORDER BY name",
		subject,
	)
	require.NoError(t, err)
	defer rows.Close()

	var result []EventRow
	for rows.Next() {
		var r EventRow
		err := rows.Scan(&r.Subject, &r.Source, &r.Producer, &r.CloudEventID, &r.Type, &r.DataVersion, &r.Name, &r.Timestamp, &r.DurationNs, &r.Metadata)
		require.NoError(t, err)
		result = append(result, r)
	}
	require.NoError(t, rows.Err())

	t.Logf("ClickHouse dimo.event: %d rows for subject=%s", len(result), subject)
	for i, r := range result {
		t.Logf("  event[%d]: subject=%s name=%s type=%s timestamp=%v durationNs=%d metadata=%s source=%s producer=%s cloudEventID=%s dataVersion=%s",
			i, r.Subject, r.Name, r.Type, r.Timestamp, r.DurationNs, r.Metadata, r.Source, r.Producer, r.CloudEventID, r.DataVersion)
	}
	return result
}

// CloudEventRow represents a row from the ClickHouse cloud_event table.
type CloudEventRow struct {
	Subject         string    `db:"subject"`
	EventTime       time.Time `db:"event_time"`
	EventType       string    `db:"event_type"`
	ID              string    `db:"id"`
	Source          string    `db:"source"`
	Producer        string    `db:"producer"`
	DataContentType string    `db:"data_content_type"`
	DataVersion     string    `db:"data_version"`
	IndexKey        string    `db:"index_key"`
	DataIndexKey    string    `db:"data_index_key"`
	VoidsID         string    `db:"voids_id"`
}

// queryCloudEvents queries the ClickHouse cloud_event table for a given subject.
// Logs full row data for each result.
func queryCloudEvents(t *testing.T, subject string) []CloudEventRow {
	t.Helper()
	indexDB, err := sql.Open("clickhouse", clickhouseIndexDSN)
	require.NoError(t, err)
	defer indexDB.Close()

	_, err = indexDB.Exec("OPTIMIZE TABLE cloud_event FINAL")
	require.NoError(t, err)

	rows, err := indexDB.Query(
		"SELECT subject, event_time, event_type, id, source, producer, data_content_type, data_version, index_key, data_index_key, voids_id FROM cloud_event WHERE subject = ? ORDER BY event_type",
		subject,
	)
	require.NoError(t, err)
	defer rows.Close()

	var result []CloudEventRow
	for rows.Next() {
		var r CloudEventRow
		err := rows.Scan(&r.Subject, &r.EventTime, &r.EventType, &r.ID, &r.Source, &r.Producer, &r.DataContentType, &r.DataVersion, &r.IndexKey, &r.DataIndexKey, &r.VoidsID)
		require.NoError(t, err)
		result = append(result, r)
	}
	require.NoError(t, rows.Err())

	t.Logf("ClickHouse cloud_event: %d rows for subject=%s", len(result), subject)
	for i, r := range result {
		t.Logf("  cloud_event[%d]: subject=%s eventType=%s eventTime=%v id=%s source=%s producer=%s dataContentType=%s dataVersion=%s indexKey=%s dataIndexKey=%s voidsId=%s",
			i, r.Subject, r.EventType, r.EventTime, r.ID, r.Source, r.Producer, r.DataContentType, r.DataVersion, r.IndexKey, r.DataIndexKey, r.VoidsID)
	}
	return result
}

// clearMinIOObjects removes all objects in the bucket with the given prefix.
// Call this before sending test data to ensure no stale parquet files from previous runs.
func clearMinIOObjects(t *testing.T, prefix string) {
	t.Helper()
	ctx := context.Background()
	objectsCh := minioClient.ListObjects(ctx, minioBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for obj := range objectsCh {
		require.NoError(t, obj.Err)
		err := minioClient.RemoveObject(ctx, minioBucket, obj.Key, minio.RemoveObjectOptions{})
		require.NoError(t, err, "failed to remove MinIO object %s", obj.Key)
	}
	t.Logf("Cleared MinIO objects with prefix %q", prefix)
}

// listMinIOObjects lists all objects in the bucket with the given prefix.
func listMinIOObjects(t *testing.T, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	var keys []string
	for obj := range minioClient.ListObjects(ctx, minioBucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		require.NoError(t, obj.Err)
		keys = append(keys, obj.Key)
	}
	return keys
}

// readParquetFromMinIO downloads a parquet file from MinIO and decodes it.
// Logs full event data for each row.
func readParquetFromMinIO(t *testing.T, key string) []cloudevent.StoredEvent {
	t.Helper()
	ctx := context.Background()
	obj, err := minioClient.GetObject(ctx, minioBucket, key, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()

	data, err := io.ReadAll(obj)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	events, err := parquet.Decode(reader, int64(len(data)))
	require.NoError(t, err)

	t.Logf("Parquet %s: %d events", key, len(events))
	for i, ev := range events {
		dataJSON, _ := json.Marshal(ev.Data)
		t.Logf("  parquet[%d]: id=%s subject=%s type=%s source=%s producer=%s specVersion=%s time=%v dataVersion=%s data=%s dataIndexKey=%s",
			i, ev.ID, ev.Subject, ev.Type, ev.Source, ev.Producer, ev.SpecVersion, ev.Time, ev.DataVersion, string(dataJSON), ev.DataIndexKey)
	}
	return events
}

// readBytesFromMinIO downloads an object from MinIO and returns the raw bytes
// alongside its Content-Type header.
func readBytesFromMinIO(t *testing.T, key string) ([]byte, string) {
	t.Helper()
	ctx := context.Background()
	obj, err := minioClient.GetObject(ctx, minioBucket, key, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()

	stat, err := obj.Stat()
	require.NoError(t, err, "failed to stat MinIO object %s", key)

	data, err := io.ReadAll(obj)
	require.NoError(t, err)

	t.Logf("MinIO %s: %d bytes, Content-Type=%q", key, len(data), stat.ContentType)
	return data, stat.ContentType
}

// parseSignalCE parses a Kafka message as a SignalCloudEvent.
func parseSignalCE(t *testing.T, msg []byte) vss.SignalCloudEvent {
	t.Helper()
	var ce vss.SignalCloudEvent
	err := json.Unmarshal(msg, &ce)
	require.NoError(t, err, "failed to parse SignalCloudEvent: %s", string(msg))
	return ce
}

// parseEventCE parses a Kafka message as an EventCloudEvent.
func parseEventCE(t *testing.T, msg []byte) vss.EventCloudEvent {
	t.Helper()
	var ce vss.EventCloudEvent
	err := json.Unmarshal(msg, &ce)
	require.NoError(t, err, "failed to parse EventCloudEvent: %s", string(msg))
	return ce
}

