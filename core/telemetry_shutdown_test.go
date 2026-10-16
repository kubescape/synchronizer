package core

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/require"
)

func TestClientCancellationReturnsFromStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{Account: "test", Cluster: "test"}))
	defer cancel()
	client, server := net.Pipe()
	defer server.Close()
	s, err := NewSynchronizerClient(ctx, nil, client, func() (net.Conn, error) { t.Error("canceled client must not reconnect"); return nil, context.Canceled })
	require.NoError(t, err)
	reading := make(chan struct{})
	originalRead := s.readDataFunc
	s.readDataFunc = func(rw io.ReadWriter) ([]byte, error) { close(reading); return originalRead(rw) }
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("client did not enter read loop")
	}
	cancel()
	require.NoError(t, s.Stop(ctx))
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("Start did not return after cancellation; deferred provider flush would be unreachable")
	}
}
