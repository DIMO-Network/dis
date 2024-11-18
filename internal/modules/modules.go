// Package modules provides a way to load varies code modules from data providers.
package modules

import (
	"context"
	"fmt"

	"github.com/DIMO-Network/dis/internal/modules/macaron"
	"github.com/DIMO-Network/dis/internal/modules/ruptela"
	"github.com/DIMO-Network/dis/internal/modules/tesla"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type Module interface {
	SetLogger(logger *service.Logger)
	SetConfig(config string) error
}

// SignalModule is an interface for converting messages to signals.
type SignalModule interface {
	Module
	SignalConvert(ctx context.Context, msgData []byte) ([]vss.Signal, error)
}

// CloudEventModule is an interface for converting messages to cloud events.
type CloudEventModule interface {
	Module
	CloudEventConvert(ctx context.Context, msgData []byte) ([][]byte, error)
}

var signalModules = map[string]func() (SignalModule, error){
	"macaron": func() (SignalModule, error) { return macaron.New() },
	"ruptela": func() (SignalModule, error) { return ruptela.New() },
	"tesla":   func() (SignalModule, error) { return tesla.New() },
}

var cloudEventModules = map[string]func() (CloudEventModule, error){
	"macaron": func() (CloudEventModule, error) { return macaron.New() },
	"ruptela": func() (CloudEventModule, error) { return ruptela.New() },
	"tesla":   func() (CloudEventModule, error) { return tesla.New() },
}

// NotFoundError is an error type for when a module is not found.
type NotFoundError string

func (e NotFoundError) Error() string {
	return string(e)
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

// LoadCloudEventModule attempts to load a specific cloudEvent module.
func LoadCloudEventModule(name string, opts Options) (CloudEventModule, error) { //nolint // I don't like returning an interface here, but we don't have a concrete type to return.
	// Load signal modules from the given path.
	moduleCtor, ok := cloudEventModules[name]
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
