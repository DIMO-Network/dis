//go:build integration

package integration

import (
	"bytes"
	"context"
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

// consumeKafka consumes all available messages from `topic` within `timeout`.
// Returns the raw message bytes.
func consumeKafka(t *testing.T, topic string, timeout time.Duration) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer cl.Close()

	var msgs [][]byte
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded {
					return msgs
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
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

// querySignals queries the ClickHouse signal table for a given subject.
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
	return result
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
func readParquetFromMinIO(t *testing.T, key string) []cloudevent.RawEvent {
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
	return events
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

