package incluster

import (
	"context"
	"sync"
	"testing"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
)

const allowAllNamespaces = `{"includeNamespaces":[],"excludeNamespaces":[]}`
const excludePayments = `{"includeNamespaces":[],"excludeNamespaces":["payments"]}`

func TestNamespaceFiltersDocuments(t *testing.T) {
	for _, tt := range []struct {
		name, document string
		skip           bool
	}{
		{"empty", allowAllNamespaces, false},
		{"exclude", excludePayments, true},
		{"include wins", `{"includeNamespaces":"payments,team-a","excludeNamespaces":"payments"}`, false},
		{"include restricts", `{"includeNamespaces":["team-a"],"excludeNamespaces":[]}`, true},
		{"include regex", `{"includeNamespaces":[],"excludeNamespaces":["payments"],"includeNamespacesRegex":["^pay"]}`, false},
		{"exclude regex", `{"includeNamespaces":[],"excludeNamespaces":[],"excludeNamespacesRegex":"^pay"}`, true},
		{"blank regex ignored", `{"includeNamespaces":[],"excludeNamespaces":[],"includeNamespacesRegex":["  "]}`, false},
		{"regex trimmed", `{"includeNamespaces":[],"excludeNamespaces":[],"excludeNamespacesRegex":[" ^pay "]}`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := &namespaceFilters{}
			changed, err := f.update([]byte(tt.document))
			require.NoError(t, err)
			require.True(t, changed)
			require.Equal(t, tt.skip, f.skip("payments"))
			changed, err = f.update([]byte(tt.document))
			require.NoError(t, err)
			require.False(t, changed)
		})
	}
	for _, invalid := range []string{
		`{`, `null`, `{}`, `[]`, `{"includeNamespaces":[]}`, `{"includeNamespaces":[],"excludeNamespaces":null}`,
		`{"includeNamespaces":[],"excludeNamespaces":[1]}`, `{"includeNamespaces":true,"excludeNamespaces":[]}`,
		`{"includeNamespaces":[],"excludeNamespaces":[],"typo":[]}`,
		`{"includeNamespaces":[],"excludeNamespaces":[],"includeNamespacesRegex":"["}`,
		`{"includeNamespaces":[],"excludeNamespaces":[],"excludeNamespacesRegex":null}`,
		allowAllNamespaces + `{}`, allowAllNamespaces + `garbage`,
	} {
		t.Run(invalid, func(t *testing.T) {
			f := &namespaceFilters{}
			_, err := f.update([]byte(excludePayments))
			require.NoError(t, err)
			_, err = f.update([]byte(invalid))
			require.Error(t, err)
			require.True(t, f.skip("payments"))
		})
	}
}

func TestNamespaceFiltersConcurrentUpdates(t *testing.T) {
	f := &namespaceFilters{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				f.skip("payments")
			}
		})
	}
	for range 100 {
		_, err := f.update([]byte(excludePayments))
		require.NoError(t, err)
		_, err = f.update([]byte(allowAllNamespaces))
		require.NoError(t, err)
	}
	wg.Wait()
}

func TestNamespaceFiltersSharedAndDisabled(t *testing.T) {
	cfg := config.InCluster{Namespace: "kubescape", NamespaceFilterConfigMapName: "filters"}
	a := NewInClusterAdapter(cfg, nil, nil)
	first := a.newClient(nil, nil, config.Resource{})
	second := a.GetClientByKind(domain.Kind{Version: "v1", Resource: "unknown"}).(*Client)
	require.Same(t, first.namespaceFilters, second.namespaceFilters)
	for _, doc := range []string{excludePayments, allowAllNamespaces, excludePayments} {
		_, err := a.namespaceFilters.update([]byte(doc))
		require.NoError(t, err)
		for _, c := range []*Client{first, second} {
			require.Equal(t, doc == excludePayments, c.liveNamespaceExcluded("payments"))
			require.False(t, c.liveNamespaceExcluded(""))
			require.False(t, c.liveNamespaceExcluded("kubescape"))
		}
	}
	cfg.NamespaceFilterConfigMapName = ""
	cfg.ExcludeNamespaces = []string{"payments"}
	disabled := NewInClusterAdapter(cfg, nil, nil)
	require.Nil(t, disabled.namespaceFilters)
	require.NoError(t, disabled.Start(context.Background())) // no resources; no API access
	c := disabled.newClient(nil, nil, config.Resource{})
	require.True(t, c.skipNamespace("payments"))
	require.False(t, c.liveNamespaceExcluded("payments"))
	cfg.NamespaceFilterConfigMapName = "filters"
	cfg.Namespace = ""
	require.ErrorContains(t, NewInClusterAdapter(cfg, nil, nil).Start(context.Background()), "namespace is required")
}

func filterTestObject(ns string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "sample", "namespace": ns, "resourceVersion": "2"},
		"spec":     map[string]any{"replicas": int64(1)},
	}}
}

func TestNamespaceFiltersOutboundPaths(t *testing.T) {
	ctx := context.Background()
	resource := config.Resource{Group: "apps", Version: "v1", Resource: "deployments", Strategy: domain.CopyStrategy}
	for _, path := range []string{"get", "verify fallback", "patch fallback", "storage bootstrap", "reconcile existing", "reconcile new", "reconcile deleted", "put dispatch", "patch dispatch", "verify dispatch", "delete dispatch"} {
		t.Run(path, func(t *testing.T) {
			obj := filterTestObject("payments")
			dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), obj)
			c := NewClient(dynamicClient, nil, config.InCluster{Namespace: "kubescape"}, resource)
			c.namespaceFilters = &namespaceFilters{}
			id := domain.KindName{Kind: c.kind, Namespace: "payments", Name: "sample", ResourceVersion: 1}
			calls := 0
			hit := func(context.Context, domain.KindName) error { calls++; return nil }
			c.RegisterCallbacks(ctx, domain.Callbacks{
				DeleteObject: hit,
				GetObject:    func(ctx context.Context, id domain.KindName, _ []byte) error { return hit(ctx, id) },
				PutObject:    func(ctx context.Context, id domain.KindName, _ string, _ []byte) error { return hit(ctx, id) },
				PatchObject:  func(ctx context.Context, id domain.KindName, _ string, _ []byte) error { return hit(ctx, id) },
				VerifyObject: func(ctx context.Context, id domain.KindName, _ string) error { return hit(ctx, id) },
			})
			for _, doc := range []string{excludePayments, allowAllNamespaces, excludePayments} {
				_, err := c.namespaceFilters.update([]byte(doc))
				require.NoError(t, err)
				before := calls
				switch path {
				case "get":
					err = c.GetObject(ctx, id, nil)
				case "verify fallback":
					err = c.VerifyObject(ctx, id, "wrong")
				case "patch fallback":
					err = c.PatchObject(ctx, id, "wrong", []byte(`{}`))
				case "storage bootstrap":
					_, err = c.getExistingStorageObjects(ctx)
				case "reconcile existing":
					err = reconcileBatchProcessingFunc(ctx, c, domain.BatchItems{NewChecksum: []domain.NewChecksum{{Kind: c.kind, Namespace: "payments", Name: "sample", ResourceVersion: 1}}})
				case "reconcile new":
					err = reconcileBatchProcessingFunc(ctx, c, domain.BatchItems{NewChecksum: []domain.NewChecksum{{Kind: c.kind, Namespace: "payments", Name: "absent", ResourceVersion: 1}}})
				case "reconcile deleted":
					err = dynamicClient.Resource(c.res).Namespace("payments").Delete(ctx, "sample", metav1.DeleteOptions{})
					if err != nil && doc != excludePayments {
						t.Fatal(err)
					}
					err = reconcileBatchProcessingFunc(ctx, c, domain.BatchItems{NewChecksum: []domain.NewChecksum{{Kind: c.kind, Namespace: "payments", Name: "sample", ResourceVersion: 1}}})
					_, createErr := dynamicClient.Resource(c.res).Namespace("payments").Create(ctx, obj.DeepCopy(), metav1.CreateOptions{})
					require.NoError(t, createErr)
				case "put dispatch":
					err = c.callPutOrPatch(ctx, id, "checksum", nil, []byte(`{}`))
				case "patch dispatch":
					_, err = c.dispatchPatchObject(ctx, id, "checksum", []byte(`{}`))
				case "verify dispatch":
					err = c.callVerifyObject(ctx, id, []byte(`{}`))
				case "delete dispatch":
					err = c.sendDeleteObject(ctx, id)
				}
				require.NoError(t, err)
				if doc == excludePayments {
					require.Equal(t, before, calls, "excluded data must not be sent")
				} else {
					require.Greater(t, calls, before, "allowed data must be sent")
				}
				_, err = dynamicClient.Resource(c.res).Namespace("payments").Get(ctx, "sample", metav1.GetOptions{})
				require.NoError(t, err, "filtering must not delete cluster resources")
			}
		})
	}
}

func TestNamespaceFiltersInboundWritesPreserved(t *testing.T) {
	ctx := context.Background()
	dynamicClient := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{{Group: "apps", Version: "v1", Resource: "deployments"}: "DeploymentList"})
	c := NewClient(dynamicClient, nil, config.InCluster{}, config.Resource{Group: "apps", Version: "v1", Resource: "deployments"})
	c.namespaceFilters = &namespaceFilters{}
	_, err := c.namespaceFilters.update([]byte(excludePayments))
	require.NoError(t, err)
	obj := filterTestObject("payments")
	data, err := obj.MarshalJSON()
	require.NoError(t, err)
	id := domain.KindName{Kind: c.kind, Namespace: "payments", Name: "sample"}
	require.NoError(t, c.PutObject(ctx, id, "", data))
	_, err = dynamicClient.Resource(c.res).Namespace("payments").Get(ctx, "sample", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, c.DeleteObject(ctx, id))
}

func TestNamespaceFiltersEventsAndPatchBaseline(t *testing.T) {
	ctx := context.Background()
	c := NewClient(nil, nil, config.InCluster{Namespace: "kubescape"}, config.Resource{Group: "apps", Version: "v1", Resource: "deployments", Strategy: domain.PatchStrategy})
	c.namespaceFilters = &namespaceFilters{}
	calls := 0
	c.RegisterCallbacks(ctx, domain.Callbacks{
		VerifyObject: func(context.Context, domain.KindName, string) error { calls++; return nil },
		PutObject:    func(context.Context, domain.KindName, string, []byte) error { calls++; return nil },
		PatchObject:  func(context.Context, domain.KindName, string, []byte) error { calls++; return nil },
		DeleteObject: func(context.Context, domain.KindName) error { calls++; return nil },
	})
	for _, ns := range []string{"payments", "kubescape", ""} {
		for _, eventType := range []watch.EventType{watch.Added, watch.Modified, watch.Deleted} {
			for _, doc := range []string{excludePayments, allowAllNamespaces, excludePayments} {
				events := make(chan watch.Event, 1)
				obj := filterTestObject(ns)
				events <- watch.Event{Type: eventType, Object: obj}
				close(events)
				// Change after enqueue, before processing: queued work must use the latest rules.
				_, err := c.namespaceFilters.update([]byte(doc))
				require.NoError(t, err)
				c.ShadowObjects = map[string][]byte{}
				before := calls
				require.NoError(t, c.processEvents(ctx, events))
				if ns == "payments" && doc == excludePayments {
					require.Equal(t, before, calls)
				} else {
					require.Equal(t, before+1, calls)
				}
			}
		}
	}
	id := domain.KindName{Kind: c.kind, Namespace: "payments", Name: "sample"}
	oldObject := []byte(`{"spec":{"replicas":1}}`)
	newObject := []byte(`{"spec":{"replicas":2}}`)
	c.ShadowObjects[id.String()] = oldObject
	_, err := c.namespaceFilters.update([]byte(excludePayments))
	require.NoError(t, err)
	require.NoError(t, c.callPutOrPatch(ctx, id, "", nil, newObject))
	require.Equal(t, oldObject, c.ShadowObjects[id.String()])
	_, err = c.namespaceFilters.update([]byte(allowAllNamespaces))
	require.NoError(t, err)
	before := calls
	require.NoError(t, c.callPutOrPatch(ctx, id, "", nil, newObject))
	require.Equal(t, before+1, calls)
	require.Equal(t, newObject, c.ShadowObjects[id.String()])
}
