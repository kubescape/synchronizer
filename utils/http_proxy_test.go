package utils

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProxyCONNECTCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	connected := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		_ = req.Body.Close()
		close(connected) // Deliberately never reply to CONNECT.
		_, _ = io.Copy(io.Discard, conn)
		close(closed)
	}()
	t.Setenv("HTTP_PROXY", "http://"+listener.Addr().String())
	t.Setenv("HTTPS_PROXY", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, err := GetDialer()(ctx, "tcp", "example.invalid:443")
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("proxy did not receive CONNECT")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("dial ignored cancellation while proxy was silent")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("canceled proxy connection was not closed")
	}
}

type waitingForwardDialer struct{ entered chan struct{} }

func (d *waitingForwardDialer) Dial(string, string) (net.Conn, error) {
	panic("context-free forward dial must not be used")
}
func (d *waitingForwardDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	close(d.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestProxyTCPDialCancellation(t *testing.T) {
	forward := &waitingForwardDialer{entered: make(chan struct{})}
	u, err := url.Parse("http://proxy.invalid:3128")
	require.NoError(t, err)
	d, err := newHTTPProxy(u, forward)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := d.(interface {
			DialContext(context.Context, string, string) (net.Conn, error)
		}).DialContext(ctx, "tcp", "example.invalid:443")
		result <- err
	}()
	select {
	case <-forward.entered:
	case <-time.After(time.Second):
		t.Fatal("forward dial did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("TCP establishment ignored cancellation")
	}
}

func TestProxyConnectionOutlivesDialContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		req, err := http.ReadRequest(r)
		if err != nil {
			return
		}
		_ = req.Body.Close()
		_, _ = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\nContent-Length: 0\r\n\r\n")
		_, _ = io.Copy(c, r)
	}()
	t.Setenv("HTTP_PROXY", "http://"+listener.Addr().String())
	t.Setenv("HTTPS_PROXY", "")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	conn, err := GetDialer()(ctx, "tcp", "example.invalid:443")
	require.NoError(t, err)
	defer conn.Close()
	<-ctx.Done() // A completed dial must detach cancellation/deadline from the tunnel.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = io.WriteString(conn, "ok")
	require.NoError(t, err)
	body := make([]byte, 2)
	_, err = io.ReadFull(conn, body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))
}
