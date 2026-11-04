package incluster

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws/wsutil"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

type replayFilterAdapter struct {
	*adapters.MockAdapter
	client  *Client
	barrier chan struct{}
}

func (a *replayFilterAdapter) RegisterCallbacks(ctx context.Context, c domain.Callbacks) {
	a.client.RegisterCallbacks(ctx, c)
}
func (a *replayFilterAdapter) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	return a.client.PatchObject(ctx, id, checksum, patch)
}
func (a *replayFilterAdapter) PutObject(context.Context, domain.KindName, string, []byte) error {
	a.barrier <- struct{}{}
	return nil
}

func TestReplayFailedPatchAfterNamespaceIncluded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{}))
	defer cancel()
	obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "command", "namespace": "payments"}}}
	client := NewClient(fake.NewSimpleDynamicClient(runtime.NewScheme(), obj), nil, config.InCluster{}, config.Resource{Version: "v1", Resource: "configmaps", Strategy: domain.PatchStrategy})
	client.namespaceFilters = &namespaceFilters{}
	_, err := client.namespaceFilters.update([]byte(excludePayments))
	require.NoError(t, err)
	adapter := &replayFilterAdapter{MockAdapter: adapters.NewMockAdapter(true), client: client, barrier: make(chan struct{}, 1)}
	local, remote := net.Pipe()
	defer remote.Close()
	syncer, err := core.NewSynchronizerClient(ctx, []adapters.Adapter{adapter}, local, nil)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- syncer.Start(ctx) }()
	defer func() { cancel(); _ = syncer.Stop(ctx); <-done }()
	event := domain.EventPatchObject
	patch, err := json.Marshal(domain.PatchObject{Event: &event, Kind: &domain.Kind{Version: "v1", Resource: "configmaps"}, Name: "command", Namespace: "payments", MsgId: "patch-id", Checksum: "wrong", Patch: "{}"})
	require.NoError(t, err)
	require.NoError(t, remote.SetDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, wsutil.WriteServerBinary(remote, patch))
	putEvent := domain.EventPutObject
	barrier, err := json.Marshal(domain.PutObject{Event: &putEvent, Kind: &domain.Kind{Version: "v1", Resource: "configmaps"}, Name: "barrier", MsgId: "barrier-id", Object: "{}"})
	require.NoError(t, err)
	require.NoError(t, wsutil.WriteServerBinary(remote, barrier))
	select {
	case <-adapter.barrier:
	case <-time.After(5 * time.Second):
		t.Fatal("first patch not processed")
	}
	_, err = client.namespaceFilters.update([]byte(allowAllNamespaces))
	require.NoError(t, err)
	require.NoError(t, wsutil.WriteServerBinary(remote, patch))
	require.NoError(t, remote.SetReadDeadline(time.Now().Add(time.Second)))
	for {
		response, err := wsutil.ReadClientBinary(remote)
		require.NoError(t, err, "replayed failed patch must request recovery")
		var got domain.Generic
		require.NoError(t, json.Unmarshal(response, &got))
		if *got.Event == domain.EventPing {
			continue
		}
		require.Equal(t, domain.EventGetObject, *got.Event)
		require.Equal(t, "patch-id", got.MsgId)
		break
	}
}
