package core

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
)

func TestSynchronizerConcurrentConnectionReplacement(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	writes := 0
	s := &Synchronizer{
		Conn:     &conn,
		isClient: true,
		newConn:  func() (net.Conn, error) { return conn, nil },
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
