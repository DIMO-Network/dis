// Package modules provides a way to load varies code modules from data providers.
package modules

import (
	"context"
	"fmt"

	"github.com/DIMO-Network/DIS/internal/modules/macaron"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// NotFoundError is an error type for when a module is not found.
type NotFoundError string

func (e NotFoundError) Error() string {
	return string(e)
}

// SignalModule is an interface for converting messages to signals.
type SignalModule interface {
	SignalConvert(ctx context.Context, msgData []byte) ([]vss.Signal, error)
	SetLogger(logger *service.Logger)
	SetConfig(config string) error
}

// Options is a struct for configuring signal modules.
type Options struct {
	Logger       *service.Logger
	FilePath     string
	ModuleConfig string
}

// LoadSignalModule attempts to load a specific signal module.
func LoadSignalModule(name string, opts Options) (SignalModule, error) { //nolint // I don't like returning an interface here, but we don't have a concrete type to return.
	// Load signal modules from the given path.
	moduleCtor, ok := signalModules[name]
	if !ok {
		return nil, NotFoundError(fmt.Sprintf("signal module '%s' not found", name))
	}
	module, err := moduleCtor()
	if err != nil {
		return nil, fmt.Errorf("failed to create module '%s': %w", name, err)
	}
	module.SetLogger(opts.Logger)
	err = module.SetConfig(opts.ModuleConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to set config for module '%s': %w", name, err)
	}
	return module, nil
}

var signalModules = map[string]func() (SignalModule, error){
	"macaron": func() (SignalModule, error) { return macaron.New() },
}
