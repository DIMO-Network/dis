package main

import (
	"context"

	// explicitly import required clickhouse dependencies.
	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/benthosdev/benthos/v4/public/service"

	// import _all_ Benthos components for third party services
	_ "github.com/benthosdev/benthos/v4/public/components/all"

	// Add our custom plugin packages here
	_ "github.com/DIMO-Network/benthos-plugin/internal/checksignature"
	_ "github.com/DIMO-Network/benthos-plugin/internal/dimovss"
)

func main() {
	service.RunCLI(context.Background())
}
