package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws"
)

func TestReconnectCancellation(t *testing.T) {
	// A reconnect is already dialing when shutdown cancels its context. The
	// caller can return independently without sharing its error with the writer.
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		done := make(chan error, 1)
		dialer := ws.Dialer{NetDial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		go func() {
			conn, err := connectWebsocket(ctx, dialer, "ws://reconnect.invalid", "test")
			if conn != nil {
				_ = conn.Close()
			}
			done <- err
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("reconnect did not enter dial")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("reconnect error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation did not release reconnect")
		}
	}
}
