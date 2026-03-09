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

	// Import pure for basic processing.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	// Import prometheus for metrics.
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"

	// Add our custom plugin packages here.
	_ "github.com/DIMO-Network/dis/internal/processors/cloudeventconvert"
	_ "github.com/DIMO-Network/dis/internal/processors/eventconvert"
	_ "github.com/DIMO-Network/dis/internal/processors/eventstoslice"
	_ "github.com/DIMO-Network/dis/internal/processors/fingerprintvalidate"
	_ "github.com/DIMO-Network/dis/internal/processors/httpinputserver"
	_ "github.com/DIMO-Network/dis/internal/processors/rawparquet"
	_ "github.com/DIMO-Network/dis/internal/processors/signalconvert"
	_ "github.com/DIMO-Network/dis/internal/processors/signalstoslice"
)

func main() {
	service.RunCLI(context.Background())
}
