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
