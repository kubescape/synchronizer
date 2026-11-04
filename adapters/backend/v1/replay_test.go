package backend

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
	"github.com/stretchr/testify/require"
)

func replayConfig(t *testing.T, settings string) config.Backend {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"backend":{"replay":`+settings+`}}`), 0600))
	cfg, err := config.LoadConfig(dir)
	require.NoError(t, err)
	return cfg.Backend
}

func collectReplay(adapter *Adapter, ctx context.Context, names *[]string) {
	adapter.RegisterCallbacks(ctx, domain.Callbacks{PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
		*names = append(*names, id.Name)
		return nil
	}})
}

func TestReplayLimits(t *testing.T) {
	for _, tc := range []struct {
		name, settings string
		want           []string
	}{
		{"disabled", `{"enabled":false}`, nil},
		{"per client", `{"enabled":true,"maxMessagesPerClient":2}`, []string{"two", "three"}},
		{"global", `{"enabled":true,"maxMessages":2}`, []string{"two", "three"}},
		{"ordered", `{"enabled":true}`, []string{"one", "two", "three"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, tc.settings))
			handler := &messaging.MessageHandler{}
			for _, name := range []string{"one", "two", "three"} {
				require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, name)))
			}
			connCtx := connectionContext("connection", time.Now(), nil)
			var names []string
			collectReplay(adapter, connCtx, &names)
			require.NoError(t, adapter.Start(connCtx))
			require.Equal(t, tc.want, names)
			require.NoError(t, adapter.Stop(connCtx))
			collectReplay(adapter, connCtx, &names)
			require.NoError(t, adapter.Start(connCtx))
			require.Equal(t, tc.want, names, "successfully dispatched backlog is removed")
			require.NoError(t, adapter.Stop(connCtx))
		})
	}
}

func TestReplayExpiryAndCleanup(t *testing.T) {
	cfg := replayConfig(t, `{"enabled":true,"ttlSeconds":3}`)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		adapter := NewBackendAdapter(ctx, silentProducer{}, cfg)
		handler := &messaging.MessageHandler{}
		require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, "expired")))
		time.Sleep(4 * time.Second)
		synctest.Wait()
		adapter.replay.mu.Lock()
		require.Zero(t, adapter.replay.bytes, "idle buffers release payload memory")
		require.Empty(t, adapter.replay.clients)
		require.Empty(t, adapter.replay.routes)
		adapter.replay.mu.Unlock()
		require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, "fresh")))
		connCtx := connectionContext("connection", time.Now(), nil)
		var names []string
		collectReplay(adapter, connCtx, &names)
		require.NoError(t, adapter.Start(connCtx))
		require.Equal(t, []string{"fresh"}, names)
		require.NoError(t, adapter.Stop(connCtx))
		require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, "shutdown")))
		cancel()
		synctest.Wait()
		require.Zero(t, adapter.replay.bytes)
		require.Empty(t, adapter.replay.clients)
	})
}

func TestReplayBytesAndTenantIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := replayMessage(t, "one")
	size := len(msg.Payload) + len(msg.ID)
	for k, v := range msg.Properties {
		size += len(k) + len(v)
	}
	adapter := NewBackendAdapter(ctx, silentProducer{}, config.Backend{Replay: &config.ReplayConfig{Enabled: true, MaxBytes: size}})
	handler := &messaging.MessageHandler{}
	require.NoError(t, handler.Handle(ctx, adapter, msg))
	// A different tenant with the same cluster shares the pod byte budget, but not its backlog.
	msg.Properties[messaging.MsgPropAccount] = "22222222-2222-3333-4444-555555555555"
	require.NoError(t, handler.Handle(ctx, adapter, msg))
	connCtx := connectionContext("one", time.Now(), nil)
	var first, second []string
	collectReplay(adapter, connCtx, &first)
	require.NoError(t, adapter.Start(connCtx))
	require.Empty(t, first, "global byte cap evicts oldest tenant's entry")
	otherID := utils.ClientIdentifierFromContext(connCtx)
	otherID.Account = msg.Properties[messaging.MsgPropAccount]
	otherCtx := context.WithValue(ctx, domain.ContextKeyClientIdentifier, otherID)
	collectReplay(adapter, otherCtx, &second)
	require.NoError(t, adapter.Start(otherCtx))
	require.Equal(t, []string{"one"}, second)
	require.NoError(t, adapter.Stop(connCtx))
	require.NoError(t, adapter.Stop(otherCtx))
	msg.Payload, _ = json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Object: make([]byte, size)})
	require.ErrorContains(t, handler.Handle(ctx, adapter, msg), "maxBytes")
}

func TestReplayRejectsMalformedMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	handler := &messaging.MessageHandler{}
	for _, payload := range []string{`{`, `{"kind":"invalid"}`, `{"kind":"/v1/configmaps","patch":123}`} {
		msg := replayMessage(t, "bad")
		msg.Payload = []byte(payload)
		require.Error(t, handler.Handle(ctx, adapter, msg))
	}
	require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, "good")))
	connCtx := connectionContext("connection", time.Now(), nil)
	var names []string
	collectReplay(adapter, connCtx, &names)
	require.NoError(t, adapter.Start(connCtx))
	require.Equal(t, []string{"good"}, names)
	require.NoError(t, adapter.Stop(connCtx))
}

func TestReplayCountsInFlightMessagesAndShutdown(t *testing.T) {
	cfg := replayConfig(t, `{"enabled":true,"maxMessages":1,"ttlSeconds":1}`)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		adapter := NewBackendAdapter(ctx, silentProducer{}, cfg)
		handler := &messaging.MessageHandler{}
		require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, "one")))
		connCtx := connectionContext("connection", time.Now(), nil)
		entered, release := make(chan struct{}), make(chan struct{})
		adapter.RegisterCallbacks(connCtx, domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error {
			close(entered)
			<-release
			return nil
		}})
		done := make(chan error, 1)
		go func() { done <- adapter.Start(connCtx) }()
		<-entered
		time.Sleep(2 * time.Second)
		other := replayMessage(t, "two")
		other.Properties[messaging.MsgPropCluster] = "other"
		require.ErrorContains(t, handler.Handle(ctx, adapter, other), "in flight", "expiry must not free the capacity occupied by an active replay")
		cancel()
		synctest.Wait()
		close(release)
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, adapter.replay.bytes)
		require.Empty(t, adapter.replay.clients)
		require.Empty(t, adapter.replay.routes)
		require.NoError(t, adapter.Stop(connCtx))
	})
}

func TestReplayReaderOrderConfiguration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		adapter := NewBackendAdapter(ctx, silentProducer{}, config.Backend{Replay: &config.ReplayConfig{Enabled: enabled}})
		want := 8
		if enabled {
			want = 1
		}
		require.Equal(t, want, messaging.ReaderWorkers(adapter, 8))
		cancel()
	}
}

func TestReplayFailedStartKeepsBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	require.NoError(t, (&messaging.MessageHandler{}).Handle(ctx, adapter, replayMessage(t, "command")))
	connCtx := connectionContext("connection", time.Now(), nil)
	require.ErrorContains(t, adapter.Start(connCtx), "no callbacks")
	require.False(t, adapter.IsRelated(connCtx, utils.ClientIdentifierFromContext(connCtx)))
	var names []string
	collectReplay(adapter, connCtx, &names)
	require.NoError(t, adapter.Start(connCtx))
	require.Equal(t, []string{"command"}, names)
	require.NoError(t, adapter.Stop(connCtx))
}

func TestReplayCopiesBrokerMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	msg := replayMessage(t, "original")
	require.NoError(t, (&messaging.MessageHandler{}).Handle(ctx, adapter, msg))
	clear(msg.Payload)
	clear(msg.Properties)
	connCtx := connectionContext("one", time.Now(), nil)
	var names []string
	collectReplay(adapter, connCtx, &names)
	require.NoError(t, adapter.Start(connCtx))
	require.Equal(t, []string{"original"}, names)
	require.NoError(t, adapter.Stop(connCtx))
}

func TestReplayFailureRetainsBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	handler := &messaging.MessageHandler{}
	for _, name := range []string{"one", "two"} {
		require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, name)))
	}
	first := connectionContext("first", time.Now(), nil)
	adapter.RegisterCallbacks(first, domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error { return errors.New("closed socket") }})
	require.ErrorContains(t, adapter.Start(first), "closed socket")
	require.ErrorContains(t, handler.Handle(ctx, adapter, replayMessage(t, "three")), "closed socket")
	require.NoError(t, adapter.Stop(first))
	second := connectionContext("second", time.Now(), nil)
	var names []string
	collectReplay(adapter, second, &names)
	require.NoError(t, adapter.Start(second))
	require.Equal(t, []string{"one", "two", "three"}, names)
	require.NoError(t, adapter.Stop(second))
}

func TestReplayRetainsFailedLiveDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	handler := &messaging.MessageHandler{}
	first := connectionContext("first", time.Now(), nil)
	adapter.RegisterCallbacks(first, domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error { return errors.New("closed socket") }})
	require.NoError(t, adapter.Start(first))
	require.ErrorContains(t, handler.Handle(ctx, adapter, replayMessage(t, "command")), "closed socket")
	require.NoError(t, adapter.Stop(first))
	second := connectionContext("second", time.Now(), nil)
	var names []string
	collectReplay(adapter, second, &names)
	require.NoError(t, adapter.Start(second))
	require.Equal(t, []string{"command"}, names)
	require.NoError(t, adapter.Stop(second))
}

func TestReplayOrdersLiveMessagesAndAllowsOtherClients(t *testing.T) {
	cfg := replayConfig(t, `{"enabled":true}`)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		adapter := NewBackendAdapter(ctx, silentProducer{}, cfg)
		handler := &messaging.MessageHandler{}
		for _, name := range []string{"one", "two"} {
			require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, name)))
		}
		connCtx := connectionContext("one", time.Now(), nil)
		entered, release := make(chan struct{}), make(chan struct{})
		var names []string
		adapter.RegisterCallbacks(connCtx, domain.Callbacks{PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
			if id.Name == "one" {
				close(entered)
				<-release
			}
			names = append(names, id.Name)
			return nil
		}})
		started := make(chan error, 1)
		go func() { started <- adapter.Start(connCtx) }()
		<-entered
		live := make(chan error, 1)
		go func() { live <- handler.Handle(ctx, adapter, replayMessage(t, "three")) }()
		select {
		case <-live:
			t.Fatal("live message overtook replay")
		default:
		}
		otherID := utils.ClientIdentifierFromContext(connCtx)
		otherID.Cluster = "other"
		otherCtx := context.WithValue(ctx, domain.ContextKeyClientIdentifier, otherID)
		var other []string
		collectReplay(adapter, otherCtx, &other)
		require.NoError(t, adapter.Start(otherCtx), "a blocked replay does not hold the global connection mutex")
		require.NoError(t, adapter.Stop(otherCtx))
		close(release)
		require.NoError(t, <-started)
		require.NoError(t, <-live)
		require.Equal(t, []string{"one", "two", "three"}, names)
		require.NoError(t, adapter.Stop(connCtx))
	})
}

func TestReplayConcurrentConnectAndMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	handler := &messaging.MessageHandler{}
	for range 100 {
		connCtx := connectionContext("connection", time.Now(), nil)
		var names []string
		collectReplay(adapter, connCtx, &names)
		done := make(chan error, 1)
		go func() { done <- handler.Handle(ctx, adapter, replayMessage(t, "command")) }()
		require.NoError(t, adapter.Start(connCtx))
		require.NoError(t, <-done)
		require.Equal(t, []string{"command"}, names)
		require.NoError(t, adapter.Stop(connCtx))
	}
}

func TestReplayStaleStopCannotRemoveEqualTimeReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	now := time.Now()
	first, second := connectionContext("first", now, nil), connectionContext("second", now, nil)
	var oldNames, newNames []string
	collectReplay(adapter, first, &oldNames)
	require.NoError(t, adapter.Start(first))
	collectReplay(adapter, second, &newNames)
	require.NoError(t, adapter.Start(second))
	require.NoError(t, adapter.Stop(first))
	require.NoError(t, (&messaging.MessageHandler{}).Handle(ctx, adapter, replayMessage(t, "live")))
	require.Equal(t, []string{"live"}, newNames)
	require.Empty(t, oldNames)
	require.NoError(t, adapter.Stop(second))
}

func replayMessage(t *testing.T, name string) messaging.IncomingMessage {
	t.Helper()
	payload, err := json.Marshal(messaging.PutObjectMessage{
		MsgId: name, Kind: "/v1/configmaps", Name: name, Object: []byte(`{"metadata":{"name":"` + name + `"}}`),
	})
	require.NoError(t, err)
	return messaging.IncomingMessage{ID: "broker-" + name, Payload: payload, Properties: map[string]string{
		messaging.MsgPropAccount: "11111111-2222-3333-4444-555555555555",
		messaging.MsgPropCluster: "cluster", messaging.MsgPropEvent: messaging.MsgPropEventValuePutObjectMessage,
	}}
}

func TestReplayAcrossPods(t *testing.T) {
	cfg := replayConfig(t, `{"enabled":true}`)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		oldPod := NewBackendAdapter(ctx, silentProducer{}, cfg)
		newPod := NewBackendAdapter(ctx, silentProducer{}, cfg)
		oldCtx := connectionContext("old", time.Now(), nil)
		oldWrites := 0
		oldPod.RegisterCallbacks(oldCtx, domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error {
			// The old socket accepts a write, but the vanished peer never applies it.
			oldWrites++
			return nil
		}})
		require.NoError(t, oldPod.Start(oldCtx))
		handler := &messaging.MessageHandler{}
		message := replayMessage(t, "command")
		require.NoError(t, handler.Handle(ctx, oldPod, message))
		require.NoError(t, handler.Handle(ctx, newPod, message))
		require.Equal(t, 1, oldWrites)
		time.Sleep(12 * time.Second)
		newCtx := connectionContext("new", time.Now(), nil)
		var applied []string
		newPod.RegisterCallbacks(newCtx, domain.Callbacks{PutObject: func(ctx context.Context, id domain.KindName, _ string, _ []byte) error {
			_, msgID := utils.DeptMsgIdFromContext(ctx)
			require.Equal(t, "command", msgID)
			applied = append(applied, id.Name)
			return nil
		}})
		require.NoError(t, newPod.Start(newCtx))
		require.Equal(t, []string{"command"}, applied)
		require.True(t, oldPod.IsRelated(oldCtx, utils.ClientIdentifierFromContext(oldCtx)), "old pod still holds its dead connection")
		time.Sleep(71 * time.Second)
		require.NoError(t, oldPod.Stop(oldCtx))
		require.NoError(t, newPod.Stop(newCtx))
	})
}
