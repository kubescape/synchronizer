package messaging

import (
	"context"
	"testing"

	"github.com/kubescape/synchronizer/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromConfig_NoRegisteredFactory(t *testing.T) {
	original := fromConfigFactory
	fromConfigFactory = nil
	t.Cleanup(func() {
		fromConfigFactory = original
	})

	components, err := NewFromConfig(context.Background(), config.Config{})
	require.NoError(t, err)
	assert.Nil(t, components)
}

func TestNewFromConfig_UsesRegisteredFactory(t *testing.T) {
	original := fromConfigFactory
	fromConfigFactory = func(_ context.Context, _ config.Config) (*Components, error) {
		return &Components{}, nil
	}
	t.Cleanup(func() {
		fromConfigFactory = original
	})

	components, err := NewFromConfig(context.Background(), config.Config{})
	require.NoError(t, err)
	assert.NotNil(t, components)
}

// the server calls this on the (nil, nil) it gets when no queue is configured, and Close is
// a plain field a backend could leave unset. either one panicking would crash the shutdown
func TestComponents_Shutdown(t *testing.T) {
	t.Run("nil components", func(t *testing.T) {
		var components *Components
		assert.NotPanics(t, components.Shutdown)
	})

	t.Run("nil close function", func(t *testing.T) {
		assert.NotPanics(t, (&Components{}).Shutdown)
	})

	t.Run("calls close", func(t *testing.T) {
		calls := 0
		(&Components{Close: func() { calls++ }}).Shutdown()
		assert.Equal(t, 1, calls)
	})
}
