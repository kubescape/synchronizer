package backend

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/require"
)

type recordedReconciliation struct {
	id      domain.ClientIdentifier
	event   string
	payload []byte
	ctx     context.Context
	at      time.Time
}

type reconciliationProducer struct {
	mu       sync.Mutex
	messages []recordedReconciliation
}

func (p *reconciliationProducer) ProduceMessage(ctx context.Context, id domain.ClientIdentifier, event string, payload []byte, _ ...map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, recordedReconciliation{id: id, event: event, payload: append([]byte(nil), payload...), ctx: ctx, at: time.Now()})
	return nil
}

func (p *reconciliationProducer) ProduceMessageWithoutIdentifier(ctx context.Context, event string, payload []byte) error {
	return p.ProduceMessage(ctx, domain.ClientIdentifier{}, event, payload)
}

func (p *reconciliationProducer) requests() []recordedReconciliation {
	p.mu.Lock()
	defer p.mu.Unlock()
	var requests []recordedReconciliation
	for _, msg := range p.messages {
		if msg.event == messaging.MsgPropEventValueReconciliationRequestMessage || msg.event == messaging.MsgPropEventValueReconciliationRequestMessageBatch {
			requests = append(requests, msg)
		}
	}
	return requests
}

func loadReconciliationConfig(t *testing.T, raw string) config.Backend {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte(raw), 0600))
	cfg, err := config.LoadConfig(dir)
	require.NoError(t, err)
	return cfg.Backend
}

func reconciliationContext(connectionID string) context.Context {
	return context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: "account", Cluster: "cluster", ConnectionId: connectionID,
		ConnectionTime: time.Now(), SyncVersion: "v0.0.57",
	})
}

func TestReconciliationAfterConnectSendsOnce(t *testing.T) {
	// No periodic schedule is configured, and sendBatch must not change the one-off message.
	cfg := loadReconciliationConfig(t, `{"backend":{"reconciliationTaskConfig":{"reconcileAfterConnectSeconds":10,"sendBatch":true}}}`)
	synctest.Test(t, func(t *testing.T) {
		producer := &reconciliationProducer{}
		adapter := NewBackendAdapter(context.Background(), producer, cfg)
		ctx := reconciliationContext("first")
		startedAt := time.Now()
		defer adapter.Stop(ctx)
		adapter.RegisterCallbacks(ctx, domain.Callbacks{})
		require.NoError(t, adapter.Start(ctx))
		synctest.Wait()
		time.Sleep(9 * time.Second)
		synctest.Wait()
		require.Empty(t, producer.requests(), "must wait for the configured delay")
		time.Sleep(4 * time.Second)
		synctest.Wait()
		requests := producer.requests()
		require.Len(t, requests, 1, "connection should trigger reconciliation even without a periodic schedule")
		require.GreaterOrEqual(t, requests[0].at.Sub(startedAt), 10*time.Second)
		require.LessOrEqual(t, requests[0].at.Sub(startedAt), 12*time.Second)
		require.NoError(t, requests[0].ctx.Err(), "timer cleanup must not cancel asynchronous broker delivery")
		require.Equal(t, messaging.MsgPropEventValueReconciliationRequestMessage, requests[0].event)
		var msg messaging.ReconciliationRequestMessage
		require.NoError(t, json.Unmarshal(requests[0].payload, &msg))
		require.Equal(t, "account", msg.Account)
		require.Equal(t, "cluster", msg.Cluster)
		require.True(t, msg.ServerInitiated)
		require.NotEmpty(t, msg.MsgId)
		time.Sleep(time.Hour)
		synctest.Wait()
		require.Len(t, producer.requests(), 1, "the connection trigger is one-shot")
	})
}

func TestReconciliationAfterConnectRejectedStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer := &reconciliationProducer{}
		adapter := NewBackendAdapter(context.Background(), producer, config.Backend{
			ReconciliationTask: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: 10},
		})
		stale := reconciliationContext("stale")
		time.Sleep(time.Second)
		current := reconciliationContext("current")
		defer adapter.Stop(current)
		adapter.RegisterCallbacks(current, domain.Callbacks{})
		require.NoError(t, adapter.Start(current))
		adapter.RegisterCallbacks(stale, domain.Callbacks{})
		require.Error(t, adapter.Start(stale))
		require.NoError(t, adapter.Stop(stale))
		time.Sleep(time.Minute)
		synctest.Wait()
		requests := producer.requests()
		require.Len(t, requests, 1)
		require.Equal(t, "current", requests[0].id.ConnectionId)
	})
}

func TestReconciliationAfterConnectFailedStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		producer := &reconciliationProducer{}
		adapter := NewBackendAdapter(context.Background(), producer, config.Backend{
			ReconciliationTask: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: 10},
		})
		ctx := reconciliationContext("missing callbacks")
		defer adapter.Stop(ctx)
		require.Error(t, adapter.Start(ctx))
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Empty(t, producer.requests())
	})
}

func TestReconciliationAfterConnectKeepsPeriodicBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mainCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		producer := &reconciliationProducer{}
		adapter := NewBackendAdapter(mainCtx, producer, config.Backend{
			ReconciliationTask: &config.ReconciliationTaskConfig{
				ReconcileAfterConnectSeconds: 10, TaskIntervalSeconds: 60,
				IntervalFromConnectionSeconds: 30, SendBatch: true,
			},
		})
		ctx := reconciliationContext("first")
		defer adapter.Stop(ctx)
		adapter.RegisterCallbacks(ctx, domain.Callbacks{})
		require.NoError(t, adapter.Start(ctx))
		synctest.Wait()
		time.Sleep(13 * time.Second)
		synctest.Wait()
		requests := producer.requests()
		require.Len(t, requests, 1, "one-off ignores the periodic minimum connection age")
		require.Equal(t, messaging.MsgPropEventValueReconciliationRequestMessage, requests[0].event)
		time.Sleep(48 * time.Second)
		synctest.Wait()
		requests = producer.requests()
		require.Len(t, requests, 2)
		require.Equal(t, messaging.MsgPropEventValueReconciliationRequestMessageBatch, requests[1].event)
	})
}

func TestReconciliationAfterConnectCancelled(t *testing.T) {
	for _, reason := range []string{"disconnect", "connection context", "server shutdown"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mainCtx, shutdown := context.WithCancel(context.Background())
				defer shutdown()
				producer := &reconciliationProducer{}
				adapter := NewBackendAdapter(mainCtx, producer, config.Backend{
					ReconciliationTask: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: 10},
				})
				ctx, cancel := context.WithCancel(reconciliationContext("first"))
				defer cancel()
				defer adapter.Stop(ctx)
				adapter.RegisterCallbacks(ctx, domain.Callbacks{})
				require.NoError(t, adapter.Start(ctx))
				synctest.Wait()
				time.Sleep(5 * time.Second)
				switch reason {
				case "disconnect":
					require.NoError(t, adapter.Stop(ctx))
				case "connection context":
					cancel()
				case "server shutdown":
					shutdown()
				}
				time.Sleep(time.Minute)
				synctest.Wait()
				require.Empty(t, producer.requests())
			})
		})
	}
}

func TestReconciliationAfterConnectSuperseded(t *testing.T) {
	for _, sameTime := range []bool{false, true} {
		name := "newer connection"
		if sameTime {
			name = "different connection with equal timestamp"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				producer := &reconciliationProducer{}
				adapter := NewBackendAdapter(context.Background(), producer, config.Backend{
					ReconciliationTask: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: 10},
				})
				first := reconciliationContext("first")
				adapter.RegisterCallbacks(first, domain.Callbacks{})
				require.NoError(t, adapter.Start(first))
				synctest.Wait()
				time.Sleep(5 * time.Second)
				second := reconciliationContext("second")
				if sameTime {
					id := second.Value(domain.ContextKeyClientIdentifier).(domain.ClientIdentifier)
					id.ConnectionTime = first.Value(domain.ContextKeyClientIdentifier).(domain.ClientIdentifier).ConnectionTime
					second = context.WithValue(second, domain.ContextKeyClientIdentifier, id)
				}
				defer adapter.Stop(second)
				adapter.RegisterCallbacks(second, domain.Callbacks{})
				require.NoError(t, adapter.Start(second))
				require.NoError(t, adapter.Stop(first), "late cleanup must leave the winning connection intact")
				synctest.Wait()
				time.Sleep(8 * time.Second)
				synctest.Wait()
				require.Empty(t, producer.requests(), "the superseded connection's deadline has passed")
				time.Sleep(5 * time.Second)
				synctest.Wait()
				requests := producer.requests()
				require.Len(t, requests, 1)
				require.Equal(t, "second", requests[0].id.ConnectionId)
			})
		})
	}
}

func TestReconciliationAfterConnectDisabled(t *testing.T) {
	maxInt64 := int64(1<<63 - 1)
	for _, tc := range []struct {
		name    string
		cfg     *config.ReconciliationTaskConfig
		version string
	}{
		{name: "absent config", version: "v0.0.57"},
		{name: "zero by default", cfg: &config.ReconciliationTaskConfig{}, version: "v0.0.57"},
		{name: "negative delay", cfg: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: -1}, version: "v0.0.57"},
		{name: "unrepresentable delay", cfg: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: int(maxInt64)}, version: "v0.0.57"},
		{name: "old client", cfg: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: 10}, version: "v0.0.56"},
		{name: "unknown version", cfg: &config.ReconciliationTaskConfig{ReconcileAfterConnectSeconds: 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				producer := &reconciliationProducer{}
				adapter := NewBackendAdapter(context.Background(), producer, config.Backend{ReconciliationTask: tc.cfg})
				ctx := reconciliationContext("first")
				id := ctx.Value(domain.ContextKeyClientIdentifier).(domain.ClientIdentifier)
				id.SyncVersion = tc.version
				ctx = context.WithValue(ctx, domain.ContextKeyClientIdentifier, id)
				defer adapter.Stop(ctx)
				adapter.RegisterCallbacks(ctx, domain.Callbacks{})
				require.NoError(t, adapter.Start(ctx))
				time.Sleep(24 * time.Hour)
				synctest.Wait()
				require.Empty(t, producer.requests())
			})
		})
	}
}
