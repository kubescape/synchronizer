package core

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gobwas/ws/wsutil"
	"github.com/kubescape/synchronizer/adapters"
	incluster "github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

type applyingAdapter struct {
	*adapters.MockAdapter
	put func(context.Context, domain.KindName, string, []byte) error
}

func TestClientDeduplicationPreservesResponseRetries(t *testing.T) {
	for _, event := range []domain.Event{domain.EventGetObject, domain.EventNewChecksum, domain.EventPatchObject} {
		t.Run(event.Value().(string), func(t *testing.T) {
			ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
			obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "command", "namespace": "default"}}}
			dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), obj)
			adapter := incluster.NewClient(dynamicClient, nil, config.InCluster{}, config.Resource{Version: "v1", Resource: "configmaps", Strategy: domain.PatchStrategy})
			local, remote := net.Pipe()
			defer remote.Close()
			client, err := NewSynchronizerClient(ctx, []adapters.Adapter{adapter}, local, nil)
			require.NoError(t, err)
			defer client.Stop(ctx)
			done := make(chan error, 1)
			go func() { done <- client.listenForSyncEvents(ctx) }()
			// The intentionally wrong checksum makes PatchObject request a full
			// object and return nil. A replay must resend that recovery request.
			data, err := json.Marshal(map[string]any{"Event": event, "Kind": domain.Kind{Version: "v1", Resource: "configmaps"}, "Name": "command", "Namespace": "default", "MsgId": "same-id", "Checksum": "wrong", "Patch": "{}"})
			require.NoError(t, err)
			for range 2 {
				require.NoError(t, remote.SetDeadline(time.Now().Add(5*time.Second)))
				require.NoError(t, wsutil.WriteServerBinary(remote, data))
				response, err := wsutil.ReadClientBinary(remote)
				require.NoError(t, err, "replayed queries and patch recovery must send another response")
				var generic domain.Generic
				require.NoError(t, json.Unmarshal(response, &generic))
				require.Equal(t, "same-id", generic.MsgId)
			}
			require.NoError(t, client.Stop(ctx))
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("reader did not stop")
			}
		})
	}
}

func TestClientDeduplicationOutcomes(t *testing.T) {
	type command struct{ name, id, object string }
	for _, tc := range []struct {
		name      string
		commands  []command
		failFirst bool
		opts      []Option
		want      int32
	}{
		{"successful duplicate", []command{{"a", "id", `{}`}, {"a", "id", `{}`}}, false, nil, 1},
		{"failed apply retries", []command{{"a", "id", `{}`}, {"a", "id", `{}`}, {"a", "id", `{}`}}, true, nil, 2},
		{"shared ID different objects", []command{{"a", "id", `{}`}, {"b", "id", `{}`}}, false, nil, 2},
		{"shared ID different payload", []command{{"a", "id", `{}`}, {"a", "id", `{"changed":true}`}}, false, nil, 2},
		{"empty ID", []command{{"a", "", `{}`}, {"a", "", `{}`}}, false, nil, 2},
		{"disabled", []command{{"a", "id", `{}`}, {"a", "id", `{}`}}, false, []Option{WithDeduplication(0, 0)}, 2},
		{"capacity evicts oldest", []command{{"a", "1", `{}`}, {"b", "2", `{}`}, {"c", "3", `{}`}, {"a", "1", `{}`}}, false, []Option{WithDeduplication(time.Minute, 2)}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
			var calls atomic.Int32
			barrier := make(chan struct{}, 1)
			adapter := &applyingAdapter{MockAdapter: adapters.NewMockAdapter(true), put: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
				if id.Name == "barrier" {
					barrier <- struct{}{}
					return nil
				}
				if calls.Add(1) == 1 && tc.failFirst {
					return errors.New("API temporarily unavailable")
				}
				return nil
			}}
			local, remote := net.Pipe()
			defer remote.Close()
			client, err := NewSynchronizerClient(ctx, []adapters.Adapter{adapter}, local, nil, tc.opts...)
			require.NoError(t, err)
			defer client.Stop(ctx)
			done := make(chan error, 1)
			go func() { done <- client.listenForSyncEvents(ctx) }()
			for _, cmd := range append(tc.commands, command{"barrier", "barrier", `{}`}) {
				event := domain.EventPutObject
				data, err := json.Marshal(domain.PutObject{Event: &event, Kind: kindDeployment.Kind, Name: cmd.name, MsgId: cmd.id, Object: cmd.object})
				require.NoError(t, err)
				require.NoError(t, remote.SetWriteDeadline(time.Now().Add(5*time.Second)))
				require.NoError(t, wsutil.WriteServerBinary(remote, data))
			}
			select {
			case <-barrier:
			case <-time.After(5 * time.Second):
				t.Fatal("missing barrier")
			}
			require.Equal(t, tc.want, calls.Load())
			require.NoError(t, client.Stop(ctx))
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("reader did not stop")
			}
		})
	}
}

func TestClientDeduplicationExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
		applied := make(chan struct{}, 4)
		adapter := &applyingAdapter{MockAdapter: adapters.NewMockAdapter(true), put: func(context.Context, domain.KindName, string, []byte) error { applied <- struct{}{}; return nil }}
		local, remote := net.Pipe()
		defer remote.Close()
		client, err := NewSynchronizerClient(ctx, []adapters.Adapter{adapter}, local, nil, WithDeduplication(time.Second, 10))
		require.NoError(t, err)
		defer client.Stop(ctx)
		go func() { _ = client.listenForSyncEvents(ctx) }()
		event := domain.EventPutObject
		data, err := json.Marshal(domain.PutObject{Event: &event, Kind: kindDeployment.Kind, Name: "command", MsgId: "id", Object: `{}`})
		require.NoError(t, err)
		require.NoError(t, wsutil.WriteServerBinary(remote, data))
		<-applied
		synctest.Wait()
		time.Sleep(time.Second)
		require.NoError(t, wsutil.WriteServerBinary(remote, data))
		<-applied
	})
}

func (a *applyingAdapter) PutObject(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	return a.put(ctx, id, checksum, object)
}

func TestClientDeduplicatesAppliedMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{}))
	defer cancel()
	var calls atomic.Int32
	barrier := make(chan struct{}, 1)
	adapter := &applyingAdapter{MockAdapter: adapters.NewMockAdapter(true), put: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
		if id.Name == "barrier" {
			barrier <- struct{}{}
		} else {
			calls.Add(1)
		}
		return nil
	}}
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	client, err := NewSynchronizerClient(ctx, []adapters.Adapter{adapter}, clientConn, nil)
	require.NoError(t, err)
	defer client.Stop(ctx)
	done := make(chan error, 1)
	go func() { done <- client.listenForSyncEvents(ctx) }()
	send := func(name, msgID string) {
		event := domain.EventPutObject
		data, err := json.Marshal(domain.PutObject{Event: &event, Kind: kindDeployment.Kind, Name: name, MsgId: msgID, Object: `{}`})
		require.NoError(t, err)
		require.NoError(t, serverConn.SetWriteDeadline(time.Now().Add(5*time.Second)))
		require.NoError(t, wsutil.WriteServerBinary(serverConn, data))
	}
	send("command", "id-1")
	send("command", "id-1")
	send("barrier", "barrier-id")
	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not process barrier")
	}
	require.Equal(t, int32(1), calls.Load(), "a replay must not re-apply a consumed command")
	require.NoError(t, client.Stop(ctx))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not stop")
	}
}
