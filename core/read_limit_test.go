package core

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeFragments sends payload as fragments of n bytes: a binary frame, continuations
// and a final one, masked when the reader is a server, the way a client sends them.
func writeFragments(w io.Writer, payload []byte, n int, masked bool) {
	for off := 0; off < len(payload); off += n {
		end := min(off+n, len(payload))
		op := ws.OpContinuation
		if off == 0 {
			op = ws.OpBinary
		}
		fr := ws.NewFrame(op, end == len(payload), payload[off:end])
		if masked {
			fr = ws.MaskFrame(fr)
		}
		if err := ws.WriteFrame(w, fr); err != nil {
			return
		}
	}
}

// readWithBound runs one bounded read against what write sends over a pipe.
func readWithBound(t *testing.T, state ws.State, bound int64, write func(w io.Writer)) ([]byte, error) {
	t.Helper()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	type result struct {
		data []byte
		err  error
	}
	got := make(chan result, 1)
	go func() {
		data, err := readBinary(state, bound)(server)
		got <- result{data, err}
	}()
	go write(client)
	select {
	case r := <-got:
		return r.data, r.err
	case <-time.After(2 * time.Second):
		t.Fatal("the read did not return")
		return nil, nil
	}
}

func TestReadBinaryRefusesAFrameLargerThanTheBound(t *testing.T) {
	const bound = 1 << 20
	// The header alone: a masked binary frame announcing a payload far beyond the bound.
	hdr := ws.Header{Fin: true, OpCode: ws.OpBinary, Masked: true, Mask: [4]byte{1, 2, 3, 4}, Length: 8 << 30}
	data, err := readWithBound(t, ws.StateServerSide, bound, func(w io.Writer) { _ = ws.WriteHeader(w, hdr) })
	require.ErrorIs(t, err, wsutil.ErrFrameTooLarge)
	assert.Nil(t, data)

	payload := bytes.Repeat([]byte("x"), 64<<10)
	data, err = readWithBound(t, ws.StateServerSide, bound, func(w io.Writer) { _ = wsutil.WriteClientBinary(w, payload) })
	require.NoError(t, err)
	assert.Equal(t, payload, data)
}

// Every frame stays under the bound, their sum does not: the read ends at the bound
// instead of joining the continuation frames into a message of any size.
func TestReadBinaryRefusesAFragmentedMessageOverTheBound(t *testing.T) {
	payload := bytes.Repeat([]byte("y"), 24)
	for name, state := range map[string]ws.State{"server reading a client": ws.StateServerSide, "client reading a server": ws.StateClientSide} {
		t.Run(name, func(t *testing.T) {
			masked := state == ws.StateServerSide
			data, err := readWithBound(t, state, 8, func(w io.Writer) { writeFragments(w, payload, 8, masked) })
			require.ErrorIs(t, err, errMessageTooLarge)
			assert.Nil(t, data)
		})
	}
}

func TestReadBinaryJoinsAFragmentedMessageWithinTheBound(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 24)
	for name, state := range map[string]ws.State{"server reading a client": ws.StateServerSide, "client reading a server": ws.StateClientSide} {
		t.Run(name, func(t *testing.T) {
			masked := state == ws.StateServerSide
			data, err := readWithBound(t, state, 24, func(w io.Writer) { writeFragments(w, payload, 8, masked) })
			require.NoError(t, err)
			assert.Equal(t, payload, data)

			// One byte past the bound, still in frames under it.
			data, err = readWithBound(t, state, 23, func(w io.Writer) { writeFragments(w, payload, 8, masked) })
			require.ErrorIs(t, err, errMessageTooLarge)
			assert.Nil(t, data)
		})
	}
}

// The constructors read through the bound they are given, and through the default
// without one.
func TestSynchronizersReadWithTheBound(t *testing.T) {
	refuses := func(t *testing.T, read func(rw io.ReadWriter) ([]byte, error), masked bool, length int64) {
		t.Helper()
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		hdr := ws.Header{Fin: true, OpCode: ws.OpBinary, Masked: masked, Mask: [4]byte{1, 2, 3, 4}, Length: length}
		go func() { _ = ws.WriteHeader(client, hdr) }()
		_, err := read(server)
		assert.True(t, errors.Is(err, wsutil.ErrFrameTooLarge), err)
	}
	accepts := func(t *testing.T, read func(rw io.ReadWriter) ([]byte, error), masked bool, payload []byte) {
		t.Helper()
		server, client := net.Pipe()
		defer server.Close()
		defer client.Close()
		go func() { writeFragments(client, payload, len(payload), masked) }()
		data, err := read(server)
		require.NoError(t, err)
		assert.Equal(t, payload, data)
	}
	noConn := func() (net.Conn, error) { return nil, errors.New("not dialed") }

	s, err := NewSynchronizerServer(t.Context(), nil, nil, WithMaxMessageBytes(16))
	require.NoError(t, err)
	refuses(t, s.readDataFunc, true, 17)
	accepts(t, s.readDataFunc, true, bytes.Repeat([]byte("s"), 16))

	c, err := NewSynchronizerClient(t.Context(), nil, nil, noConn, WithMaxMessageBytes(16))
	require.NoError(t, err)
	refuses(t, c.readDataFunc, false, 17)
	accepts(t, c.readDataFunc, false, bytes.Repeat([]byte("c"), 16))

	d, err := NewSynchronizerServer(t.Context(), nil, nil)
	require.NoError(t, err)
	refuses(t, d.readDataFunc, true, DefaultMaxMessageBytes+1)
	z, err := NewSynchronizerClient(t.Context(), nil, nil, noConn, WithMaxMessageBytes(0))
	require.NoError(t, err)
	refuses(t, z.readDataFunc, false, DefaultMaxMessageBytes+1)
}
