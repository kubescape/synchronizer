package messaging

import (
	"context"

	"github.com/kubescape/synchronizer/config"
)

type Components struct {
	Producer MessageProducer
	Reader   MessageReader
	Close    func()
}

type factoryFunc func(ctx context.Context, cfg config.Config) (*Components, error)

var fromConfigFactory factoryFunc

// RegisterFromConfigFactory registers the config-driven factory implementation.
// Called from backend init to avoid an import cycle.
func RegisterFromConfigFactory(f factoryFunc) {
	fromConfigFactory = f
}

// NewFromConfig creates message queue components from configuration. The context bounds
// any connection attempt made while building them, so a shutdown signal during startup
// is not ignored. Returns (nil, nil) when no message queue backend is configured.
func NewFromConfig(ctx context.Context, cfg config.Config) (*Components, error) {
	if fromConfigFactory == nil {
		return nil, nil
	}
	return fromConfigFactory(ctx, cfg)
}
