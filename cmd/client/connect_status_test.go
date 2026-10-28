package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/gobwas/ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dialAnswering returns a dialer whose every attempt is answered with the given HTTP
// status instead of an upgrade, and counts the attempts.
func dialAnswering(status string, attempts *int32, onAttempt func(n int32)) ws.Dialer {
	return ws.Dialer{Header: ws.HandshakeHeaderHTTP(http.Header{}), NetDial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		n := atomic.AddInt32(attempts, 1)
		if onAttempt != nil {
			onAttempt(n)
		}
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			if _, err := http.ReadRequest(bufio.NewReader(server)); err != nil {
				return
			}
			_, _ = io.WriteString(server, "HTTP/1.1 "+status+"\r\nContent-Length: 0\r\n\r\n")
		}()
		return client, nil
	}}
}

// A 503 means the server could not reach its authentication server, which says nothing
// about the credentials: the client keeps backing off. A 401 is a refusal and stays final.
func TestConnectWebsocketRetriesA503AndStopsOnA401(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts int32
	dialer := dialAnswering("503 Service Unavailable", &attempts, func(n int32) {
		if n == 2 {
			cancel()
		}
	})
	_, err := connectWebsocket(ctx, dialer, "ws://auth.invalid", "test")
	require.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(2), "the 503 was not retried")

	var refusals int32
	_, err = connectWebsocket(context.Background(), dialAnswering("401 Unauthorized", &refusals, nil), "ws://auth.invalid", "test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server rejected our credentials")
	assert.Equal(t, int32(1), atomic.LoadInt32(&refusals), "a refusal is final")
}
