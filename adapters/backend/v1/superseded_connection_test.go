package backend

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type silentProducer struct{}

func (silentProducer) ProduceMessage(context.Context, domain.ClientIdentifier, string, []byte, ...map[string]string) error {
	return nil
}
func (silentProducer) ProduceMessageWithoutIdentifier(context.Context, string, []byte) error {
	return nil
}

func connectionContext(connectionID string, at time.Time, closer func()) context.Context {
	ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account:        "11111111-2222-3333-4444-555555555555",
		Cluster:        "cluster",
		ConnectionId:   connectionID,
		ConnectionTime: at,
	})
	if closer != nil {
		ctx = utils.ContextWithConnectionCloser(ctx, closer)
	}
	return ctx
}

func putObjectOf(callbacks domain.Callbacks) uintptr {
	return reflect.ValueOf(callbacks.PutObject).Pointer()
}

// A newer connection from the same client ends the one it supersedes at once, keeps its
// own closer, and an older connection arriving out of order neither clobbers the live
// connection's callbacks nor closes anything.
func TestAdapterStartClosesTheSupersededConnection(t *testing.T) {
	adapter := NewBackendAdapter(context.Background(), silentProducer{}, config.Backend{})
	now := time.Now()
	closed := map[string]int{}
	closer := func(id string) func() { return func() { closed[id]++ } }

	first := connectionContext("first", now, closer("first"))
	firstCallbacks := domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error { return nil }}
	adapter.RegisterCallbacks(first, firstCallbacks)
	require.NoError(t, adapter.Start(first))

	second := connectionContext("second", now.Add(time.Second), closer("second"))
	secondCallbacks := domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error { return nil }}
	adapter.RegisterCallbacks(second, secondCallbacks)
	require.NoError(t, adapter.Start(second))
	assert.Equal(t, 1, closed["first"], "the superseded connection is closed when the newer one starts")
	assert.Equal(t, 0, closed["second"])

	// The old connection's handler winds down after the close: nothing of the new one goes.
	require.NoError(t, adapter.Stop(first))
	assert.True(t, adapter.IsRelated(second, utils.ClientIdentifierFromContext(second)))
	got, err := adapter.Callbacks(second)
	require.NoError(t, err)
	assert.Equal(t, putObjectOf(secondCallbacks), putObjectOf(got))

	// An older connection arriving late registers its callbacks before Start refuses it.
	stale := connectionContext("stale", now.Add(-time.Second), closer("stale"))
	adapter.RegisterCallbacks(stale, domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error { return nil }})
	require.Error(t, adapter.Start(stale))
	got, err = adapter.Callbacks(second)
	require.NoError(t, err)
	assert.Equal(t, putObjectOf(secondCallbacks), putObjectOf(got), "the live connection's callbacks stay in place")
	assert.Equal(t, 0, closed["second"], "the live connection is not closed by a stale one")
	assert.Equal(t, 0, closed["stale"])

	require.NoError(t, adapter.Stop(second))
	assert.Equal(t, 0, closed["second"], "Stop leaves closing to the handler")
}
