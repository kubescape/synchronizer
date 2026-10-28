package core

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A server ends a connection whose peer stays silent past the read timeout, so a
// half-open connection from a vanished node does not stay in the maps forever.
func TestServerReadDeadlineEndsASilentConnection(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: "11111111-2222-3333-4444-555555555555",
		Cluster: "cluster",
	})
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	s, err := NewSynchronizerServer(ctx, []adapters.Adapter{adapters.NewMockAdapter(false)}, serverConn, WithReadTimeout(200*time.Millisecond))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("the silent connection was not ended")
	}
}

func TestReadTimeoutDefaults(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
	server, err := NewSynchronizerServer(ctx, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, DefaultServerReadTimeout, server.readTimeout)
	client, err := NewSynchronizerClient(ctx, nil, nil, func() (net.Conn, error) { return nil, nil })
	require.NoError(t, err)
	assert.Zero(t, client.readTimeout, "the server does not ping, so a client never times a read out")
}
