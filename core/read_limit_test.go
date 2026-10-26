package core

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A frame header that claims more than the bound is refused before its payload is read,
// so a peer cannot make this side allocate what it announces; a message within the bound
// still arrives whole.
func TestReadBinaryRefusesAFrameLargerThanTheBound(t *testing.T) {
	const bound = 1 << 20
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	read := readBinary(ws.StateServerSide, bound)

	type result struct {
		data []byte
		err  error
	}
	got := make(chan result, 1)
	go func() {
		data, err := read(server)
		got <- result{data, err}
	}()
	// The header alone: a masked binary frame announcing a payload far beyond the bound.
	hdr := ws.Header{Fin: true, OpCode: ws.OpBinary, Masked: true, Mask: [4]byte{1, 2, 3, 4}, Length: 8 << 30}
	require.NoError(t, ws.WriteHeader(client, hdr))
	select {
	case r := <-got:
		require.ErrorIs(t, r.err, wsutil.ErrFrameTooLarge)
		assert.Nil(t, r.data)
	case <-time.After(2 * time.Second):
		t.Fatal("the read did not refuse the frame from its header")
	}

	server2, client2 := net.Pipe()
	defer server2.Close()
	defer client2.Close()
	payload := bytes.Repeat([]byte("x"), 64<<10)
	go func() {
		_ = wsutil.WriteClientBinary(client2, payload)
	}()
	data, err := read(server2)
	require.NoError(t, err)
	assert.Equal(t, payload, data)
}

// The server and client synchronizers read through the bounded reader.
func TestSynchronizersReadWithTheBound(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	s, err := NewSynchronizerServer(t.Context(), nil, server)
	require.NoError(t, err)
	hdr := ws.Header{Fin: true, OpCode: ws.OpBinary, Masked: true, Mask: [4]byte{1, 2, 3, 4}, Length: int64(maxMessageBytes) + 1}
	go func() { _ = ws.WriteHeader(client, hdr) }()
	_, err = s.readDataFunc(server)
	assert.True(t, errors.Is(err, wsutil.ErrFrameTooLarge), err)
}
