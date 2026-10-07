package core

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSynchronizerConcurrentConnectionReplacement(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	writes := 0
	s := &Synchronizer{
		Conn:     &conn,
		isClient: true,
		newConn: func() (net.Conn, error) {
			replacement, peer := net.Pipe()
			t.Cleanup(func() { replacement.Close(); peer.Close() })
			return replacement, nil
		},
		writeDataFunc: func(io.Writer, []byte) error {
			writes++
			if writes%2 == 1 {
				return errors.New("reconnect")
			}
			return nil
		},
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			s.sendData(context.Background(), nil)
		}
	})
	wg.Go(func() {
		for range 10000 {
			if s.connection() == nil {
				t.Error("nil connection")
			}
		}
	})
	wg.Wait()
}

// Record deadline changes while retaining the behavior of a real connection.
type deadlineRecordingConn struct {
	net.Conn
	deadlines []time.Time
	resetErr  error
}

func (c *deadlineRecordingConn) SetWriteDeadline(deadline time.Time) error {
	c.deadlines = append(c.deadlines, deadline)
	if deadline.IsZero() && c.resetErr != nil {
		return c.resetErr
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func TestWriteDataClearsDeadline(t *testing.T) {
	writeErr := errors.New("write failed")
	resetErr := errors.New("deadline reset failed")
	for _, tc := range []struct {
		name                        string
		writeErr, resetErr, wantErr error
	}{
		{name: "successful write"},
		{name: "failed write", writeErr: writeErr, wantErr: writeErr},
		{name: "failed reset", resetErr: resetErr, wantErr: resetErr},
		{name: "write error preserved on failed reset", writeErr: writeErr, resetErr: resetErr, wantErr: writeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipe, peer := net.Pipe()
			defer pipe.Close()
			defer peer.Close()
			conn := &deadlineRecordingConn{Conn: pipe, resetErr: tc.resetErr}
			wrote := false
			s := &Synchronizer{writeDataFunc: func(w io.Writer, data []byte) error {
				wrote = true
				if w != conn || string(data) != "message" {
					t.Error("writer received unexpected connection or payload")
				}
				if len(conn.deadlines) != 1 || !conn.deadlines[0].After(time.Now()) {
					t.Error("write did not have a future deadline")
				}
				return tc.writeErr
			}}
			if err := s.writeData(conn, []byte("message")); !errors.Is(err, tc.wantErr) {
				t.Fatalf("writeData error = %v, want %v", err, tc.wantErr)
			}
			if !wrote {
				t.Fatal("writer was not called")
			}
			if len(conn.deadlines) != 2 || !conn.deadlines[1].IsZero() {
				t.Fatalf("deadline was not cleared after write: %v", conn.deadlines)
			}
		})
	}
}
