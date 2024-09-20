package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import aws for s3 output.
	_ "github.com/redpanda-data/connect/v4/public/components/aws"

	// Import sql for clickhouse output.
	_ "github.com/redpanda-data/connect/v4/public/components/sql"

	// Import io for http endpoints.
	_ "github.com/redpanda-data/connect/v4/public/components/io"

	// Import prometheus for metrics.
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"

	// Add our custom plugin packages here.
	_ "github.com/DIMO-Network/dis/internal/cloudeventconvert"
	_ "github.com/DIMO-Network/dis/internal/signalconvert"
)

func main() {
	service.RunCLI(context.Background())
}
