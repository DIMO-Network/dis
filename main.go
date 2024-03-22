package main

import (
	"context"

	// explicitylu import clickhouse-go to include updated dependencies
	_ "github.com/ClickHouse/clickhouse-go/v2"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/kafka"
	_ "github.com/benthosdev/benthos/v4/public/components/sql"
	"github.com/benthosdev/benthos/v4/public/service"
)

func main() {
	service.RunCLI(context.Background())
}
