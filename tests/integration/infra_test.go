//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	indexmigrations "github.com/DIMO-Network/cloudevent/clickhouse/migrations"
	"github.com/DIMO-Network/model-garage/pkg/migrations"
	"github.com/pressly/goose/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var kafkaTopics = []string{
	"topic.device.signals",
	"topic.device.events",
}

func setupClickHouse(ctx context.Context) error {
	var err error
	clickhouseDB, err = sql.Open("clickhouse", clickhouseDSN)
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	if err := clickhouseDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}
	// Run model-garage migrations
	if err := migrations.RunGoose(ctx, []string{"up"}, clickhouseDB); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	// Run cloud_event index migrations.
	// In prod these run in a separate database (CLICKHOUSE_INDEX_DATABASE), but in tests
	// we use the same "dimo" database. Use a separate goose table to avoid version collisions
	// with the signal migrations above.
	goose.SetTableName("goose_db_version_cloud_event")
	if err := indexmigrations.RunGoose(ctx, []string{"up"}, clickhouseDB); err != nil {
		return fmt.Errorf("run cloud_event migrations: %w", err)
	}
	goose.SetTableName("goose_db_version")
	return nil
}

func setupKafka(ctx context.Context) error {
	var err error
	kafkaClient, err = kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	if err := kafkaClient.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}

	kafkaAdminClient = kadm.NewClient(kafkaClient)
	// Create topics with 1 partition each (ignore "already exists" errors)
	for _, topic := range kafkaTopics {
		_, err := kafkaAdminClient.CreateTopic(ctx, 1, 1, nil, topic)
		if err != nil && !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
			return fmt.Errorf("create topic %s: %w", topic, err)
		}
	}
	return nil
}

func setupMinIO(ctx context.Context) error {
	var err error
	minioClient, err = minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		return fmt.Errorf("create minio client: %w", err)
	}
	exists, err := minioClient.BucketExists(ctx, minioBucket)
	if err != nil {
		return fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
	}
	return nil
}
