package core

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/adapters"
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

// Startup is held before pool creation so Stop deterministically wins. This
// covers the boundary the connected-client regression intentionally avoids.
type pausedStartupAdapter struct {
	adapters.Adapter
	entered chan struct{}
	resume  chan struct{}
}

func (a *pausedStartupAdapter) Start(context.Context) error { close(a.entered); <-a.resume; return nil }
func (a *pausedStartupAdapter) Stop(context.Context) error  { return nil }

func TestStopDuringStartupRejectsLatePool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{}))
	defer cancel()
	client, peer := net.Pipe()
	defer peer.Close()
	a := &pausedStartupAdapter{Adapter: adapters.NewMockAdapter(true), entered: make(chan struct{}), resume: make(chan struct{})}
	s, err := NewSynchronizerClient(ctx, []adapters.Adapter{a}, client, func() (net.Conn, error) { return nil, context.Canceled })
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	<-a.entered
	cancel()
	require.NoError(t, s.Stop(ctx))
	close(a.resume)
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("startup did not return after Stop")
	}
	s.connMu.RLock()
	pool := s.inPool
	s.connMu.RUnlock()
	if pool != nil {
		t.Cleanup(pool.Release)
	}
	require.Nil(t, pool, "shutdown must reject a pool created after Stop completed")
}

func TestClientStopRacesWithPoolStartup(t *testing.T) {
	for n := 0; n < 100; n++ {
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{}))
		client, peer := net.Pipe()
		s, err := NewSynchronizerClient(ctx, nil, client, func() (net.Conn, error) { return nil, context.Canceled })
		require.NoError(t, err)
		begin := make(chan struct{})
		done := make(chan error, 1)
		stopped := make(chan error, 1)
		go func() { <-begin; done <- s.Start(ctx) }()
		go func() { <-begin; cancel(); stopped <- s.Stop(ctx) }()
		close(begin)
		require.NoError(t, <-stopped)
		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("concurrent Start/Stop did not complete")
		}
		s.connMu.RLock()
		pool := s.inPool
		s.connMu.RUnlock()
		if pool != nil {
			closed := pool.IsClosed()
			pool.Release()
			require.True(t, closed, "published pool must be released")
		}
		_ = peer.Close()
	}
}
