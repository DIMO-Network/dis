package main

import (
	"context"

	// explicitly import required clickhouse dependencies.
	_ "github.com/ClickHouse/clickhouse-go/v2"

	// required for the plugin to be registerd and imported.
	_ "github.com/DIMO-Network/benthos-plugin/dimovss"
	_ "github.com/benthosdev/benthos/v4/public/components/elasticsearch"
	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/kafka"
	_ "github.com/benthosdev/benthos/v4/public/components/sql"

	"github.com/benthosdev/benthos/v4/public/service"
)

func main() {
	service.RunCLI(context.Background())
}
