package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewAPIAuth_RequiresPodNamespace(t *testing.T) {
	t.Setenv("KS_API_AUTH_DISABLED", "")
	t.Setenv("POD_NAMESPACE", "")
	mw, err := newAPIAuth(context.Background(), nil)
	require.Nil(t, mw)
	require.ErrorContains(t, err, "POD_NAMESPACE must be set")
}
