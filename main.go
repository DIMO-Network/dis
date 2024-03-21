package main

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"

	// import _all_ Benthos components for third party services
	_ "github.com/benthosdev/benthos/v4/public/components/all"

	// Add our custom plugin packages here
	_ "github.com/DIMO-Network/benthos-plugin/processor"
)

func main() {
	service.RunCLI(context.Background())
}
