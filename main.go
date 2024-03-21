package main

import (
	"context"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	"github.com/benthosdev/benthos/v4/public/service"

	// Add your plugin packages here
	_ "github.com/DIMO-Network/benthos-plugin/processor"
)

func main() {
	service.RunCLI(context.Background())
}
