package core

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws/wsutil"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// When the server side of the connection goes away, the client reconnects from the
// read loop at once, without waiting for its next write, and reads from the new
// connection: a message the server sends in that time is not lost.
func TestClientReconnectsWhenReadFails(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: "11111111-2222-3333-4444-555555555555",
		Cluster: "cluster",
	})
	first, firstPeer := net.Pipe()
	second, secondPeer := net.Pipe()
	t.Cleanup(func() {
		_ = first.Close()
		_ = firstPeer.Close()
		_ = second.Close()
		_ = secondPeer.Close()
	})
	var dials atomic.Int32
	s, err := NewSynchronizerClient(ctx, []adapters.Adapter{adapters.NewMockAdapter(true)}, first, func() (net.Conn, error) {
		dials.Add(1)
		return second, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Stop(ctx) })
	var writes atomic.Int32
	s.writeDataFunc = func(io.Writer, []byte) error {
		writes.Add(1)
		return nil
	}
	reads := make(chan net.Conn, 4)
	readBinary := s.readDataFunc
	s.readDataFunc = func(rw io.ReadWriter) ([]byte, error) {
		data, err := readBinary(rw)
		if err == nil {
			reads <- rw.(net.Conn)
		}
		return data, err
	}
	go func() { _ = s.listenForSyncEvents(ctx) }()

	// the server closes its end: the client's read gets EOF
	require.NoError(t, firstPeer.Close())
	require.Eventually(t, func() bool { return s.connection() == second }, 2*time.Second, 10*time.Millisecond,
		"the client did not reconnect after a failed read")
	assert.Equal(t, int32(1), dials.Load())
	assert.Zero(t, writes.Load(), "the reconnect must not wait for a write")
	assert.False(t, s.disconnected.Load())

	// the server sends on the new connection and the client reads it
	event := domain.EventPing
	ping, err := json.Marshal(domain.Generic{Event: &event, MsgId: "after-reconnect"})
	require.NoError(t, err)
	go func() { _ = wsutil.WriteServerBinary(secondPeer, ping) }()
	select {
	case conn := <-reads:
		assert.Equal(t, second, conn)
	case <-time.After(2 * time.Second):
		t.Fatal("the client did not read from the new connection")
	}
}

// A read and a write that fail on the same connection open one new connection, not two.
func TestConcurrentReconnectsDialOnce(t *testing.T) {
	old, oldPeer := net.Pipe()
	t.Cleanup(func() { _ = old.Close(); _ = oldPeer.Close() })
	var dials atomic.Int32
	s := &Synchronizer{
		Conn:     &old,
		isClient: true,
		newConn: func() (net.Conn, error) {
			dials.Add(1)
			// keep the dial open long enough that both callers wait on it
			time.Sleep(50 * time.Millisecond)
			conn, peer := net.Pipe()
			t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
			return conn, nil
		},
	}
	var wg sync.WaitGroup
	got := make([]net.Conn, 8)
	for i := range got {
		wg.Go(func() {
			conn, err := s.reconnect(context.Background(), old)
			assert.NoError(t, err)
			got[i] = conn
		})
	}
	wg.Wait()
	assert.Equal(t, int32(1), dials.Load())
	for _, conn := range got {
		assert.Equal(t, s.connection(), conn)
	}
	assert.NotEqual(t, old, s.connection())
}

// After shutdown a failed connection is not replaced.
func TestReconnectAfterStopDoesNotDial(t *testing.T) {
	old, oldPeer := net.Pipe()
	t.Cleanup(func() { _ = old.Close(); _ = oldPeer.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &Synchronizer{
		Conn:     &old,
		isClient: true,
		newConn: func() (net.Conn, error) {
			t.Error("dialed after shutdown")
			return nil, nil
		},
	}
	_, err := s.reconnect(ctx, old)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, old, s.connection())
}

// A server that accepts the connection and closes it at once does not make the client
// dial in a loop: reconnects from the read loop wait for the backoff.
func TestReadReconnectsBackOff(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: "11111111-2222-3333-4444-555555555555",
		Cluster: "cluster",
	})
	closedConn := func() net.Conn {
		conn, peer := net.Pipe()
		_ = peer.Close()
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	var dials atomic.Int32
	s, err := NewSynchronizerClient(ctx, []adapters.Adapter{adapters.NewMockAdapter(true)}, closedConn(), func() (net.Conn, error) {
		dials.Add(1)
		return closedConn(), nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Stop(ctx) })
	go func() { _ = s.listenForSyncEvents(ctx) }()

	time.Sleep(time.Second)
	// the default backoff starts at 0.5 s, so about 2 dials in a second; allow jitter
	n := dials.Load()
	assert.GreaterOrEqual(t, n, int32(1), "the client did not reconnect")
	assert.LessOrEqual(t, n, int32(5), "the client reconnects in a loop without backoff")
}
