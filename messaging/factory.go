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

// Shutdown runs the backend cleanup, which flushes buffered producer records instead of
// dropping them. Does nothing on a nil *Components (no queue configured) or a nil Close.
func (c *Components) Shutdown() {
	if c == nil || c.Close == nil {
		return
	}
	c.Close()
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
