package incluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func filterConfigMap(document string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "filters", "namespace": "kubescape"},
		"data":     map[string]any{namespaceFiltersKey: document},
	}}
}

func filterFakeClient(objects ...runtime.Object) *fake.FakeDynamicClient {
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{configMapResource: "ConfigMapList"}, objects...)
}

func runFilterWatcher(t *testing.T, client *fake.FakeDynamicClient) (*namespaceFilterWatcher, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w, err := newNamespaceFilterWatcher(client, "kubescape", "filters", &namespaceFilters{})
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { defer close(done); w.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("watcher did not stop")
		}
	})
	return w, ctx
}

func waitFilter(t *testing.T, w *namespaceFilterWatcher, skip bool) {
	t.Helper()
	require.Eventually(t, func() bool { return w.filters.current.Load() != nil && w.filters.skip("payments") == skip }, 5*time.Second, 10*time.Millisecond)
}

func TestNamespaceFilterWatcherLifecycle(t *testing.T) {
	client := filterFakeClient()
	w, ctx := runFilterWatcher(t, client)
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	require.ErrorIs(t, w.waitForReady(waitCtx), context.DeadlineExceeded)
	cancel()
	_, err := client.Resource(configMapResource).Namespace("kubescape").Create(ctx, filterConfigMap(excludePayments), metav1.CreateOptions{})
	require.NoError(t, err)
	waitFilter(t, w, true)
	readyCtx, readyCancel := context.WithTimeout(ctx, time.Second)
	defer readyCancel()
	require.NoError(t, w.waitForReady(readyCtx))
	for _, document := range []string{`{`, `{"includeNamespaces":[]}`} {
		_, err = client.Resource(configMapResource).Namespace("kubescape").Update(ctx, filterConfigMap(document), metav1.UpdateOptions{})
		require.NoError(t, err)
	}
	// Exercise rejection synchronously as well so retention assertions cannot race delivery.
	w.apply(filterConfigMap(`{`))
	missing := filterConfigMap(allowAllNamespaces)
	delete(missing.Object, "data")
	w.apply(missing)
	other := filterConfigMap(allowAllNamespaces)
	other.SetName("other")
	w.apply(other)
	other.SetName("filters")
	other.SetNamespace("other")
	w.apply(other)
	require.True(t, w.filters.skip("payments"))
	require.NoError(t, client.Resource(configMapResource).Namespace("kubescape").Delete(ctx, "filters", metav1.DeleteOptions{}))
	require.True(t, w.filters.skip("payments"))
	_, err = client.Resource(configMapResource).Namespace("kubescape").Create(ctx, filterConfigMap(allowAllNamespaces), metav1.CreateOptions{})
	require.NoError(t, err)
	waitFilter(t, w, false)
	_, err = client.Resource(configMapResource).Namespace("kubescape").Update(ctx, filterConfigMap(excludePayments), metav1.UpdateOptions{})
	require.NoError(t, err)
	waitFilter(t, w, true)
	for _, action := range client.Actions() {
		switch action.GetVerb() {
		case "list":
			require.Equal(t, "kubescape", action.GetNamespace())
			require.Equal(t, "metadata.name=filters", action.(ktesting.ListAction).GetListRestrictions().Fields.String())
		case "watch":
			require.Equal(t, "metadata.name=filters", action.(ktesting.WatchAction).GetWatchRestrictions().Fields.String())
		}
	}
}

func TestNamespaceFilterWatcherRetriesAndReconnects(t *testing.T) {
	client := filterFakeClient(filterConfigMap(excludePayments))
	var lists, watches atomic.Int32
	first, second := watch.NewRaceFreeFake(), watch.NewRaceFreeFake()
	client.PrependReactor("list", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if lists.Add(1) == 1 {
			return true, nil, fmt.Errorf("API temporarily unavailable")
		}
		return false, nil, nil
	})
	client.PrependWatchReactor("configmaps", func(ktesting.Action) (bool, watch.Interface, error) {
		if watches.Add(1) == 1 {
			return true, first, nil
		}
		return true, second, nil
	})
	w, _ := runFilterWatcher(t, client)
	waitFilter(t, w, true)
	require.Eventually(t, func() bool { return watches.Load() >= 1 }, 5*time.Second, 10*time.Millisecond)
	first.Stop()
	require.Eventually(t, func() bool { return watches.Load() >= 2 }, 5*time.Second, 10*time.Millisecond)
	second.Modify(filterConfigMap(allowAllNamespaces))
	waitFilter(t, w, false)
	require.GreaterOrEqual(t, lists.Load(), int32(2))
}

func TestNamespaceFilterWatcherStartupInvalidAndCancellation(t *testing.T) {
	client := filterFakeClient(filterConfigMap(`{`))
	w, ctx := runFilterWatcher(t, client)
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, w.waitForReady(waitCtx), context.DeadlineExceeded)
	_, err := client.Resource(configMapResource).Namespace("kubescape").Update(ctx, filterConfigMap(allowAllNamespaces), metav1.UpdateOptions{})
	require.NoError(t, err)
	waitFilter(t, w, false)
	canceled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, w.waitForReady(canceled), context.Canceled)
}

func TestNamespaceFilterAdapterLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := filterFakeClient()
	a := NewInClusterAdapter(config.InCluster{Namespace: "kubescape", NamespaceFilterConfigMapName: "filters"}, client, nil)
	started := make(chan error, 1)
	go func() { started <- a.Start(ctx) }()
	select {
	case err := <-started:
		t.Fatalf("started without valid filters: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	_, err := client.Resource(configMapResource).Namespace("kubescape").Create(ctx, filterConfigMap(allowAllNamespaces), metav1.CreateOptions{})
	require.NoError(t, err)
	select {
	case err := <-started:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("adapter did not start")
	}
	require.NoError(t, a.Stop(ctx))
	select {
	case <-a.filterDone:
	default:
		t.Fatal("watcher still running")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	a = NewInClusterAdapter(config.InCluster{Namespace: "kubescape", NamespaceFilterConfigMapName: "filters"}, filterFakeClient(), nil)
	require.ErrorIs(t, a.Start(canceled), context.Canceled)
}

func TestNamespaceFilterAdapterStopWhileWaiting(t *testing.T) {
	ctx := context.Background()
	client := filterFakeClient()
	listed := make(chan struct{}, 1)
	client.PrependReactor("list", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		select {
		case listed <- struct{}{}:
		default:
		}
		return false, nil, nil
	})
	a := NewInClusterAdapter(config.InCluster{Namespace: "kubescape", NamespaceFilterConfigMapName: "filters"}, client, nil)
	started := make(chan error, 1)
	go func() { started <- a.Start(ctx) }()
	select {
	case <-listed:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not list")
	}
	require.NoError(t, a.Stop(ctx))
	select {
	case err := <-started:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("startup wait did not stop")
	}
	require.NoError(t, a.Stop(ctx))
}
