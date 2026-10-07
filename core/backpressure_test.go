package core

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
)

func TestDispatchCancellationWhileWriterStalled(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	entered := make(chan struct{})
	unblock := make(chan struct{})
	defer close(unblock)
	a := adapters.NewMockAdapter(true)
	s, err := newSynchronizer(ctx, []adapters.Adapter{a}, conn, true, nil, func(io.Writer, []byte) error {
		close(entered)
		<-unblock
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop(ctx)
	if err := s.dispatch(context.Background(), []byte("first")); err != nil {
		t.Fatal(err)
	}
	<-entered
	requestCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.dispatch(requestCtx, []byte("second")) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatch ignored cancellation")
	}
	callbacks, _ := a.Callbacks(context.Background())
	if !callbacks.BackendAvailable() {
		t.Fatal("initial connection unavailable")
	}
	s.markDisconnected(conn)
	if callbacks.BackendAvailable() {
		t.Fatal("failed connection available")
	}
	s.Stop(ctx)
	if callbacks.BackendAvailable() {
		t.Fatal("stopped connection available")
	}
	if err := s.dispatch(context.Background(), nil); err == nil {
		t.Fatal("dispatch accepted after stop")
	}
}

func TestBackendUnavailableAfterReadFailure(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	a := adapters.NewMockAdapter(false)
	s, err := newSynchronizer(ctx, []adapters.Adapter{a}, conn, false,
		func(io.ReadWriter) ([]byte, error) { return nil, io.EOF },
		func(io.Writer, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop(ctx)
	callbacks, _ := a.Callbacks(ctx)
	if !callbacks.BackendAvailable() {
		t.Fatal("initial connection unavailable")
	}
	if err := s.listenForSyncEvents(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("read failure: %v", err)
	}
	if callbacks.BackendAvailable() {
		t.Fatal("read failure did not mark backend unavailable")
	}
}

func TestBackendAvailabilityDuringReconnect(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	replacement, replacementPeer := net.Pipe()
	defer replacement.Close()
	defer replacementPeer.Close()
	a := adapters.NewMockAdapter(true)
	writes := 0
	s, err := newSynchronizer(ctx, []adapters.Adapter{a}, conn, true, nil,
		func(w io.Writer, _ []byte) error {
			writes++
			if w == conn {
				return io.ErrClosedPipe
			}
			if w != replacement {
				t.Error("unexpected connection")
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop(ctx)
	callbacks, _ := a.Callbacks(ctx)
	s.newConn = func() (net.Conn, error) {
		if callbacks.BackendAvailable() {
			t.Error("write failure did not mark backend unavailable")
		}
		return replacement, nil
	}
	s.sendData(ctx, []byte("message"))
	if writes != 2 {
		t.Fatalf("writes = %d, want 2", writes)
	}
	if !callbacks.BackendAvailable() {
		t.Fatal("successful reconnect did not restore availability")
	}
	s.markDisconnected(conn)
	if !callbacks.BackendAvailable() {
		t.Fatal("old connection failure poisoned replacement")
	}
	s.markDisconnected(replacement)
	if callbacks.BackendAvailable() {
		t.Fatal("current connection failure did not mark backend unavailable")
	}
}

func TestStopDuringReconnectClosesLateConnection(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	replacement, replacementPeer := net.Pipe()
	defer replacement.Close()
	defer replacementPeer.Close()
	entered := make(chan struct{})
	unblock := make(chan struct{}, 1)
	defer close(unblock)
	writes := 0
	a := adapters.NewMockAdapter(true)
	s, err := newSynchronizer(ctx, []adapters.Adapter{a}, conn, true, nil,
		func(io.Writer, []byte) error {
			writes++
			return io.ErrClosedPipe
		})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop(ctx)
	s.newConn = func() (net.Conn, error) {
		close(entered)
		<-unblock
		return replacement, nil
	}
	done := make(chan struct{})
	go func() {
		s.sendData(s.workerCtx, []byte("message"))
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not attempt reconnect")
	}
	if err := replacementPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	unblock <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer did not stop after reconnect returned")
	}
	if s.connection() != conn {
		t.Fatal("late connection installed after stop")
	}
	if writes != 1 {
		t.Fatalf("writes = %d, want only the original write", writes)
	}
	if _, err := replacementPeer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("late connection was not closed: %v", err)
	}
	callbacks, _ := a.Callbacks(ctx)
	if callbacks.BackendAvailable() {
		t.Fatal("stopped synchronizer became available")
	}
}

func TestStopClosesConnectionAndUnblocksWriter(t *testing.T) {
	for _, reconnect := range []bool{false, true} {
		name := "original connection"
		if reconnect {
			name = "published replacement"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			active, activePeer := conn, peer
			if reconnect {
				active, activePeer = net.Pipe()
				defer active.Close()
				defer activePeer.Close()
			}
			entered := make(chan struct{})
			s, err := newSynchronizer(ctx, nil, conn, true, nil, func(w io.Writer, data []byte) error {
				if w != active {
					return io.ErrClosedPipe
				}
				close(entered)
				_, err := w.Write(data)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Stop(ctx)
			s.newConn = func() (net.Conn, error) { return active, nil }
			done := make(chan struct{})
			go func() {
				s.sendData(s.workerCtx, []byte("message"))
				close(done)
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("writer did not reach active connection")
			}
			if s.connection() != active {
				t.Fatal("writer is not using the published connection")
			}
			if err := activePeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := s.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Stop did not unblock the active writer")
			}
			if _, err := activePeer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("active connection was not closed: %v", err)
			}
		})
	}
}

func TestReplacementWriteTimeoutReconnectsAndRestoresAvailability(t *testing.T) {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	original, originalPeer := net.Pipe()
	defer original.Close()
	defer originalPeer.Close()
	failed, failedPeer := net.Pipe()
	defer failed.Close()
	defer failedPeer.Close()
	recovered, recoveredPeer := net.Pipe()
	defer recovered.Close()
	defer recoveredPeer.Close()
	a := adapters.NewMockAdapter(true)
	failedWrites := 0
	s, err := newSynchronizer(ctx, []adapters.Adapter{a}, original, true, nil, func(w io.Writer, _ []byte) error {
		switch w {
		case original:
			return io.ErrClosedPipe
		case failed:
			failedWrites++
			if failedWrites == 1 {
				return os.ErrDeadlineExceeded
			}
			return nil // A timed-out connection can otherwise succeed on its next write.
		case recovered:
			return nil
		default:
			t.Error("unexpected connection")
			return io.ErrClosedPipe
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop(ctx)
	callbacks, _ := a.Callbacks(ctx)
	reconnects := 0
	s.newConn = func() (net.Conn, error) {
		reconnects++
		if callbacks.BackendAvailable() {
			t.Error("failed connection is available")
		}
		if reconnects == 1 {
			return failed, nil
		}
		return recovered, nil
	}
	s.sendData(s.workerCtx, []byte("message"))
	if reconnects != 2 || s.connection() != recovered {
		t.Fatalf("reconnects = %d, replacement recovery missing", reconnects)
	}
	if !callbacks.BackendAvailable() {
		t.Fatal("recovered backend remains unavailable")
	}
	if err := failed.SetWriteDeadline(time.Now()); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("failed replacement was not closed: %v", err)
	}
}
