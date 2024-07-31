package main

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import all Benthos components for third party services.
	_ "github.com/benthosdev/benthos/v4/public/components/all"

	// Add our custom plugin packages here.
	_ "github.com/DIMO-Network/benthos-plugin/internal/checksignature"
	_ "github.com/DIMO-Network/benthos-plugin/internal/dimovss"
	_ "github.com/DIMO-Network/benthos-plugin/internal/nameindexer"
)

func main() {
	service.RunCLI(context.Background())
}
