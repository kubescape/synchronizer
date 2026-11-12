package incluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	helpersv1 "github.com/kubescape/k8s-interface/instanceidhandler/v1/helpers"
	"github.com/kubescape/storage/pkg/apis/softwarecomposition/v1beta1"
	storagefake "github.com/kubescape/storage/pkg/generated/clientset/versioned/fake"
	spdxv1beta1 "github.com/kubescape/storage/pkg/generated/clientset/versioned/typed/softwarecomposition/v1beta1"
	storageutils "github.com/kubescape/storage/pkg/utils"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// The tests in this file provide a cluster-free unit safety net for the
// ContainerProfile sync path in client.go. They construct a Client backed by a
// fake storage clientset and a fake dynamic client so the routing and
// reconcile/put/patch/delete flows can be exercised without a live apiserver.
//
// NOTE: TestClient_watchRetry (in client_test.go) still requires a rancher/k3s
// testcontainer and cannot run locally; run the tests here with a -run filter
// that excludes it, e.g.
//
//	go test -run 'TestChoose|TestCP_|TestReconcile' ./adapters/incluster/v1/

const (
	cpGroup    = "spdx.softwarecomposition.kubescape.io"
	cpVersion  = "v1beta1"
	cpResource = "containerprofiles"
)

var cpGVR = schema.GroupVersionResource{Group: cpGroup, Version: cpVersion, Resource: cpResource}

// newContainerProfile builds a typed ContainerProfile. When checksum is
// non-empty it is stored under the sync-checksum annotation so getChecksum uses
// its fast path (no re-fetch/hash required).
func newContainerProfile(ns, name, resourceVersion, checksum string) *v1beta1.ContainerProfile {
	ann := map[string]string{}
	if checksum != "" {
		ann[helpersv1.SyncChecksumMetadataKey] = checksum
	}
	return &v1beta1.ContainerProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			ResourceVersion: resourceVersion,
			Annotations:     ann,
		},
	}
}

// cpUnstructured builds an unstructured ContainerProfile suitable for seeding
// the fake dynamic client.
func cpUnstructured(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": cpGroup + "/" + cpVersion,
		"kind":       "ContainerProfile",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": ns,
		},
	}}
}

// newCPDynamicClient returns a fake dynamic client that knows the CP list kind.
func newCPDynamicClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{cpGVR: "ContainerProfileList"},
		objs...,
	)
}

// newCPClient builds a Client wired to the CP resource with the supplied fakes.
func newCPClient(storage spdxv1beta1.SpdxV1beta1Interface, dyn dynamic.Interface, strategy domain.Strategy) *Client {
	return &Client{
		dynamicClient: dyn,
		storageClient: storage,
		kind:          &domain.Kind{Group: cpGroup, Version: cpVersion, Resource: cpResource},
		res:           cpGVR,
		ShadowObjects: map[string][]byte{},
		Strategy:      strategy,
	}
}

// TestChooseListerWatcherGetResource_ContainerProfiles asserts that the
// containerprofiles cases in chooseLister / chooseWatcher / getResource route to
// the storage client's ContainerProfiles(...).List/.Watch/.Get.
func TestChooseListerWatcherGetResource_ContainerProfiles(t *testing.T) {
	cs := storagefake.NewClientset(newContainerProfile("ns1", "cp1", "1", ""))
	c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)

	// chooseLister -> ContainerProfiles("").List
	listObj, err := c.chooseLister(metav1.ListOptions{})
	require.NoError(t, err)
	cpl, ok := listObj.(*v1beta1.ContainerProfileList)
	require.True(t, ok, "expected *ContainerProfileList, got %T", listObj)
	require.Len(t, cpl.Items, 1)
	assert.Equal(t, "cp1", cpl.Items[0].Name)

	// chooseWatcher -> ContainerProfiles("").Watch
	w, err := c.chooseWatcher(metav1.ListOptions{})
	require.NoError(t, err)
	require.NotNil(t, w)
	w.Stop()

	// getResource -> ContainerProfiles(ns).Get
	obj, err := c.getResource("ns1", "cp1")
	require.NoError(t, err)
	cp, ok := obj.(*v1beta1.ContainerProfile)
	require.True(t, ok, "expected *ContainerProfile, got %T", obj)
	assert.Equal(t, "cp1", cp.Name)

	// all three routed through the storage client on the containerprofiles resource
	var sawList, sawWatch, sawGet bool
	for _, a := range cs.Actions() {
		if a.GetResource().Resource != cpResource {
			continue
		}
		switch a.GetVerb() {
		case "list":
			sawList = true
			assert.Equal(t, "", a.GetNamespace(), "list uses ContainerProfiles(\"\")")
		case "watch":
			sawWatch = true
			assert.Equal(t, "", a.GetNamespace(), "watch uses ContainerProfiles(\"\")")
		case "get":
			sawGet = true
			assert.Equal(t, "ns1", a.GetNamespace(), "get uses ContainerProfiles(ns)")
		}
	}
	assert.True(t, sawList, "expected a list action on containerprofiles")
	assert.True(t, sawWatch, "expected a watch action on containerprofiles")
	assert.True(t, sawGet, "expected a get action on containerprofiles")
}

// TestChooseRouting_NonCPFallsThroughToDynamic asserts that a non-CP resource is
// served by the dynamic client and never touches the storage client, even when a
// storage client is present.
func TestChooseRouting_NonCPFallsThroughToDynamic(t *testing.T) {
	cs := storagefake.NewClientset()
	depGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	dep := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]interface{}{"name": "dep1", "namespace": "ns1"},
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{depGVR: "DeploymentList"},
		dep,
	)
	c := &Client{
		dynamicClient: dyn,
		storageClient: cs.SpdxV1beta1(),
		kind:          &domain.Kind{Group: "apps", Version: "v1", Resource: "deployments"},
		res:           depGVR,
		ShadowObjects: map[string][]byte{},
	}

	listObj, err := c.chooseLister(metav1.ListOptions{})
	require.NoError(t, err)
	_, ok := listObj.(*unstructured.UnstructuredList)
	assert.True(t, ok, "expected dynamic *UnstructuredList, got %T", listObj)

	w, err := c.chooseWatcher(metav1.ListOptions{})
	require.NoError(t, err)
	require.NotNil(t, w)
	w.Stop()

	obj, err := c.getResource("ns1", "dep1")
	require.NoError(t, err)
	assert.Equal(t, "dep1", obj.GetName())

	assert.Empty(t, cs.Actions(), "storage client must not be touched for non-CP resources")
}

// TestCP_PutObject_CreateAndUpdate asserts PutObject writes a ContainerProfile
// through the dynamic client (create path) and handles the already-exists update
// path on a second call.
func TestCP_PutObject_CreateAndUpdate(t *testing.T) {
	ctx := context.Background()
	dyn := newCPDynamicClient()
	c := newCPClient(storagefake.NewClientset().SpdxV1beta1(), dyn, domain.CopyStrategy)

	objBytes, err := c.filterAndMarshal(newContainerProfile("ns1", "cp1", "", ""))
	require.NoError(t, err)
	id := domain.KindName{Kind: c.kind, Name: "cp1", Namespace: "ns1"}

	// create path
	require.NoError(t, c.PutObject(ctx, id, "chk", objBytes))
	got, err := dyn.Resource(cpGVR).Namespace("ns1").Get(ctx, "cp1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "cp1", got.GetName())

	// second put hits already-exists -> get + update path, still succeeds
	require.NoError(t, c.PutObject(ctx, id, "chk", objBytes))
}

// TestCP_CallPutOrPatch_PutThenPatch asserts the PatchStrategy branch of
// callPutOrPatch: first observation of an object sends a PutObject, a subsequent
// changed object sends a (non-empty) PatchObject computed against the shadow copy.
func TestCP_CallPutOrPatch_PutThenPatch(t *testing.T) {
	ctx := context.Background()
	c := newCPClient(storagefake.NewClientset().SpdxV1beta1(), newCPDynamicClient(), domain.PatchStrategy)

	var puts int
	var patches [][]byte
	c.callbacks = domain.Callbacks{
		PutObject: func(_ context.Context, _ domain.KindName, _ string, _ []byte) error {
			puts++
			return nil
		},
		PatchObject: func(_ context.Context, _ domain.KindName, _ string, patch []byte) error {
			patches = append(patches, patch)
			return nil
		},
	}

	id := domain.KindName{Kind: c.kind, Name: "cp1", Namespace: "ns1"}
	obj1, err := c.filterAndMarshal(newContainerProfile("ns1", "cp1", "", ""))
	require.NoError(t, err)

	cp2 := newContainerProfile("ns1", "cp1", "", "")
	cp2.Labels = map[string]string{"foo": "bar"}
	obj2, err := c.filterAndMarshal(cp2)
	require.NoError(t, err)

	// first: no shadow -> PutObject, shadow gets populated
	require.NoError(t, c.callPutOrPatch(ctx, id, "chk", nil, obj1))
	assert.Equal(t, 1, puts)
	assert.Empty(t, patches)
	assert.Contains(t, c.ShadowObjects, id.String())

	// second: shadow present, object changed -> PatchObject with a real patch
	require.NoError(t, c.callPutOrPatch(ctx, id, "", nil, obj2))
	assert.Equal(t, 1, puts, "no additional PutObject expected")
	require.Len(t, patches, 1)
	assert.False(t, emptyPatch.Match(patches[0]), "patch must not be an empty resourceVersion-only patch")
	assert.Contains(t, string(patches[0]), "foo", "patch should carry the changed label")
}

// TestCP_VerifyObject_MatchAndMismatch exercises verifyObject (reads the CP via
// the storage client, hashes it) and the exported VerifyObject wrapper which
// falls back to GetObject on a checksum mismatch.
func TestCP_VerifyObject_MatchAndMismatch(t *testing.T) {
	ctx := context.Background()
	cs := storagefake.NewClientset(newContainerProfile("ns1", "cp1", "1", ""))
	c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)
	id := domain.KindName{Kind: c.kind, Name: "cp1", Namespace: "ns1"}

	// compute the expected checksum exactly as production does
	fetched, err := c.getResource("ns1", "cp1")
	require.NoError(t, err)
	expectedBytes, err := c.filterAndMarshal(fetched)
	require.NoError(t, err)
	expected, err := storageutils.CanonicalHash(expectedBytes)
	require.NoError(t, err)

	// matching checksum -> returns object, no error
	got, err := c.verifyObject(id, expected)
	require.NoError(t, err)
	assert.JSONEq(t, string(expectedBytes), string(got))

	// wrong checksum -> error
	_, err = c.verifyObject(id, "deadbeef")
	assert.Error(t, err)

	// exported wrapper: mismatch triggers GetObject callback
	var getCalled bool
	c.callbacks = domain.Callbacks{
		GetObject: func(_ context.Context, _ domain.KindName, _ []byte) error {
			getCalled = true
			return nil
		},
	}
	require.NoError(t, c.VerifyObject(ctx, id, "deadbeef"))
	assert.True(t, getCalled, "mismatch should fall back to GetObject")

	// match -> no fallback
	getCalled = false
	require.NoError(t, c.VerifyObject(ctx, id, expected))
	assert.False(t, getCalled, "matching checksum should not call GetObject")
}

// TestCP_PatchObject drives PatchObject end to end: read the base CP from
// storage, apply the merge patch, verify the checksum, then write the result via
// the dynamic client. Also covers the mismatch fallback to GetObject.
func TestCP_PatchObject(t *testing.T) {
	ctx := context.Background()

	t.Run("apply patch and put", func(t *testing.T) {
		cs := storagefake.NewClientset(newContainerProfile("ns1", "cp1", "1", ""))
		dyn := newCPDynamicClient()
		c := newCPClient(cs.SpdxV1beta1(), dyn, domain.PatchStrategy)
		id := domain.KindName{Kind: c.kind, Name: "cp1", Namespace: "ns1"}

		base, err := c.filterAndMarshal(mustGet(t, c))
		require.NoError(t, err)
		patch := []byte(`{"metadata":{"labels":{"foo":"bar"}}}`)
		modified, err := jsonpatch.MergePatch(base, patch)
		require.NoError(t, err)
		checksum, err := storageutils.CanonicalHash(modified)
		require.NoError(t, err)

		require.NoError(t, c.PatchObject(ctx, id, checksum, patch))

		// shadow updated to the patched object
		assert.JSONEq(t, string(modified), string(c.ShadowObjects[id.String()]))
		// dynamic client received the patched object with the new label
		stored, err := dyn.Resource(cpGVR).Namespace("ns1").Get(ctx, "cp1", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, "bar", stored.GetLabels()["foo"])
	})

	t.Run("checksum mismatch falls back to GetObject", func(t *testing.T) {
		cs := storagefake.NewClientset(newContainerProfile("ns1", "cp1", "1", ""))
		c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.PatchStrategy)
		id := domain.KindName{Kind: c.kind, Name: "cp1", Namespace: "ns1"}
		var getCalled bool
		c.callbacks = domain.Callbacks{
			GetObject: func(_ context.Context, _ domain.KindName, _ []byte) error {
				getCalled = true
				return nil
			},
		}
		require.NoError(t, c.PatchObject(ctx, id, "wrongchecksum", []byte(`{"metadata":{"labels":{"foo":"bar"}}}`)))
		assert.True(t, getCalled, "mismatch should fall back to GetObject")
	})
}

func mustGet(t *testing.T, c *Client) metav1.Object {
	t.Helper()
	obj, err := c.getResource("ns1", "cp1")
	require.NoError(t, err)
	return obj
}

// TestCP_DeleteObject asserts DeleteObject removes the CP via the dynamic client
// and evicts the shadow copy under PatchStrategy.
func TestCP_DeleteObject(t *testing.T) {
	ctx := context.Background()
	dyn := newCPDynamicClient(cpUnstructured("ns1", "cp1"))
	c := newCPClient(storagefake.NewClientset().SpdxV1beta1(), dyn, domain.PatchStrategy)
	id := domain.KindName{Kind: c.kind, Name: "cp1", Namespace: "ns1"}
	c.ShadowObjects[id.String()] = []byte(`{}`)

	require.NoError(t, c.DeleteObject(ctx, id))

	_, err := dyn.Resource(cpGVR).Namespace("ns1").Get(ctx, "cp1", metav1.GetOptions{})
	assert.True(t, k8sErrors.IsNotFound(err), "object should be deleted from the dynamic client")
	assert.NotContains(t, c.ShadowObjects, id.String(), "shadow copy should be evicted")
}

// TestCP_GetObjectFromMeta asserts that for the CP group getObjectFromMeta
// re-fetches the object from the storage client and marshals it.
func TestCP_GetObjectFromMeta(t *testing.T) {
	cs := storagefake.NewClientset(newContainerProfile("ns1", "cp1", "1", ""))
	c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)

	got, err := c.getObjectFromMeta(newContainerProfile("ns1", "cp1", "", ""))
	require.NoError(t, err)

	fetched, err := c.getResource("ns1", "cp1")
	require.NoError(t, err)
	want, err := c.filterAndMarshal(fetched)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got))
}

// TestCP_GetExistingStorageObjects asserts the initial list-and-verify path used
// on Start: every CP fetched from the storage client is reported via
// VerifyObject with its checksum, and a resource version is returned.
func TestCP_GetExistingStorageObjects(t *testing.T) {
	ctx := context.Background()
	cs := storagefake.NewClientset(
		newContainerProfile("ns1", "cp1", "5", "aaa"),
		newContainerProfile("ns1", "cp2", "7", "bbb"),
	)
	c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)

	verified := map[string]string{}
	c.callbacks = domain.Callbacks{
		VerifyObject: func(_ context.Context, id domain.KindName, checksum string) error {
			verified[id.Name] = checksum
			return nil
		},
	}

	rv, err := c.getExistingStorageObjects(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"cp1": "aaa", "cp2": "bbb"}, verified)
	assert.Contains(t, []string{"5", "7"}, rv)
}

// TestReconcileBatchProcessingFunc drives the reconciliation batch across the
// three divergence cases: server-only -> delete, changed version -> put,
// client-only -> verify. Client state comes from the fake storage client; server
// state comes from the NewChecksum items.
func TestReconcileBatchProcessingFunc(t *testing.T) {
	ctx := context.Background()

	t.Run("delete, put and verify are dispatched correctly", func(t *testing.T) {
		cs := storagefake.NewClientset(
			newContainerProfile("ns1", "keep", "5", "k"),
			newContainerProfile("ns1", "changed", "5", "c"),
			newContainerProfile("ns1", "onlyclient", "5", "o"),
		)
		c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)

		var deletes, puts, verifies []string
		c.callbacks = domain.Callbacks{
			DeleteObject: func(_ context.Context, id domain.KindName) error {
				deletes = append(deletes, id.Name)
				return nil
			},
			PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
				puts = append(puts, id.Name)
				return nil
			},
			VerifyObject: func(_ context.Context, id domain.KindName, _ string) error {
				verifies = append(verifies, id.Name)
				return nil
			},
		}

		items := domain.BatchItems{NewChecksum: []domain.NewChecksum{
			{Kind: c.kind, Name: "keep", Namespace: "ns1", ResourceVersion: 5, Checksum: "k"},
			{Kind: c.kind, Name: "changed", Namespace: "ns1", ResourceVersion: 6, Checksum: "c"},
			{Kind: c.kind, Name: "onlyserver", Namespace: "ns1", ResourceVersion: 9, Checksum: "s"},
		}}

		require.NoError(t, reconcileBatchProcessingFunc(ctx, c, items))
		assert.Equal(t, []string{"onlyserver"}, deletes)
		assert.Equal(t, []string{"changed"}, puts)
		assert.Equal(t, []string{"onlyclient"}, verifies)
	})

	t.Run("client-only object is verified with its stored checksum", func(t *testing.T) {
		// The storage server lists metadata only (StorageImpl.GetList), so the list
		// item carries the sync-checksum annotation but not the spec. The checksum sent
		// for an object missing from the server must be the object's own checksum, the
		// one the startup path sends, not a hash of that spec-less list item.
		stored := newContainerProfile("ns1", "onlyclient", "5", "")
		stored.Spec.Syscalls = []string{"read", "write"}
		c := newCPClient(nil, newCPDynamicClient(), domain.CopyStrategy)
		full, err := c.filterAndMarshal(stored.DeepCopy())
		require.NoError(t, err)
		checksum, err := storageutils.CanonicalHash(full)
		require.NoError(t, err)
		stored.Annotations[helpersv1.SyncChecksumMetadataKey] = checksum

		cs := storagefake.NewClientset(stored)
		cs.PrependReactor("list", cpResource, func(k8stesting.Action) (bool, runtime.Object, error) {
			item := stored.DeepCopy()
			item.Spec = v1beta1.ContainerProfileSpec{}
			return true, &v1beta1.ContainerProfileList{Items: []v1beta1.ContainerProfile{*item}}, nil
		})
		c = newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)
		verified := map[string]string{}
		c.callbacks = domain.Callbacks{
			DeleteObject: func(context.Context, domain.KindName) error { return nil },
			VerifyObject: func(_ context.Context, id domain.KindName, checksum string) error {
				verified[id.Name] = checksum
				return nil
			},
		}

		_, err = c.getExistingStorageObjects(ctx)
		require.NoError(t, err)
		require.Equal(t, checksum, verified["onlyclient"], "startup sends the stored checksum")
		delete(verified, "onlyclient")

		items := domain.BatchItems{NewChecksum: []domain.NewChecksum{
			{Kind: c.kind, Name: "onlyserver", Namespace: "ns1", ResourceVersion: 9, Checksum: "s"},
		}}
		require.NoError(t, reconcileBatchProcessingFunc(ctx, c, items))
		assert.Equal(t, checksum, verified["onlyclient"], "reconciliation should send the same checksum")
	})

	t.Run("empty NewChecksum is rejected", func(t *testing.T) {
		c := newCPClient(storagefake.NewClientset().SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)
		err := reconcileBatchProcessingFunc(ctx, c, domain.BatchItems{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})
}

// TestCP_WatchRetry_ReconnectsPromptlyAfterHealthyWatch: a watch that delivered events
// and was then closed, as the API server does when a watch reaches its timeout, should be
// re-established after the initial backoff interval (0.5s ±50%), not after an interval
// that keeps growing over the life of the process. The storage server ignores the
// resourceVersion and only streams events after the watch starts, so events during the
// wait are lost.
func TestCP_WatchRetry_ReconnectsPromptlyAfterHealthyWatch(t *testing.T) {
	cs := storagefake.NewSimpleClientset() //nolint:staticcheck // SA1019: see TestCP_WatchRetry_ViaFakeStorageWatcher
	watchers := make(chan *watch.FakeWatcher)
	cs.PrependWatchReactor(cpResource, func(k8stesting.Action) (bool, watch.Interface, error) {
		w := watch.NewFake()
		watchers <- w
		return true, w, nil
	})
	c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)
	eq := utils.NewCooldownQueue()
	go func() {
		for range eq.ResultChan {
		}
	}()
	// the goroutine is left running, see TestCP_WatchRetry_ViaFakeStorageWatcher
	go c.watchRetry(context.Background(), metav1.ListOptions{}, eq)

	w := <-watchers
	for i := range 6 {
		w.Add(newContainerProfile("ns1", fmt.Sprintf("cp-%d", i), fmt.Sprint(i+1), ""))
		w.Stop()
		closed := time.Now()
		select {
		case w = <-watchers:
		case <-time.After(time.Second):
			t.Fatalf("watch %d: not re-established within 1s after a healthy watch closed", i+2)
		}
		t.Logf("watch %d re-established after %s", i+2, time.Since(closed).Round(time.Millisecond))
	}
}

// TestCP_WatchRetry_KeepsBackingOffAfterEventThenError: a watch that delivers an event
// and then fails, over and over, must still back off exponentially. Only a watch that
// ends normally after delivering events restarts after the initial interval.
func TestCP_WatchRetry_KeepsBackingOffAfterEventThenError(t *testing.T) {
	cs := storagefake.NewSimpleClientset() //nolint:staticcheck // SA1019: see TestCP_WatchRetry_ViaFakeStorageWatcher
	watchers := make(chan *watch.FakeWatcher)
	cs.PrependWatchReactor(cpResource, func(k8stesting.Action) (bool, watch.Interface, error) {
		w := watch.NewFake()
		watchers <- w
		return true, w, nil
	})
	c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)
	eq := utils.NewCooldownQueue()
	go func() {
		for range eq.ResultChan {
		}
	}()
	// the goroutine is left running, see TestCP_WatchRetry_ViaFakeStorageWatcher
	go c.watchRetry(context.Background(), metav1.ListOptions{}, eq)

	// The backoff starts at 0.5s and grows by 1.5x with ±50% jitter, so the 5th retry
	// waits at least 0.5*1.5^4*0.5 ≈ 1.27s. Resetting on every event would keep it
	// around 0.5s.
	w := <-watchers
	var last time.Duration
	for i := range 5 {
		w.Add(newContainerProfile("ns1", fmt.Sprintf("cp-%d", i), fmt.Sprint(i+1), ""))
		w.Error(&metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonExpired})
		failed := time.Now()
		select {
		case w = <-watchers:
		case <-time.After(10 * time.Second):
			t.Fatalf("watch %d: not re-established within 10s", i+2)
		}
		last = time.Since(failed)
		t.Logf("watch %d re-established after %s", i+2, last.Round(time.Millisecond))
	}
	assert.Greater(t, last, time.Second, "event-then-error watches should keep backing off")
}

// TestCP_WatchRetry_ViaFakeStorageWatcher exercises watchRetry against the CP
// path using the in-memory fake storage watcher, proving the happy path (choose
// watcher -> forward event to the cooldown queue) without a k3s testcontainer.
//
// The watchRetry goroutine is intentionally left running: closing the cooldown
// queue would drive it into its os.Exit(1) giving-up branch, so instead we let
// it block on the still-open fake watcher until the test process ends.
func TestCP_WatchRetry_ViaFakeStorageWatcher(t *testing.T) {
	// Use NewSimpleClientset here because storage v0.0.290's generated applyconfiguration
	// TypeConverter does not include ContainerProfile, causing NewClientset's field-managed
	// Create to fail prior to watch dispatch.
	cs := storagefake.NewSimpleClientset() //nolint:staticcheck // SA1019: NewClientset lacks ContainerProfile applyconfig in v0.0.290
	c := newCPClient(cs.SpdxV1beta1(), newCPDynamicClient(), domain.CopyStrategy)
	eq := utils.NewCooldownQueue()
	ctx := context.Background()

	go c.watchRetry(ctx, metav1.ListOptions{}, eq)

	// Continuously create CPs until an event is observed. This avoids a race
	// between watch establishment and object creation (only future events are
	// delivered by the fake watcher).
	done := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			_, _ = cs.SpdxV1beta1().ContainerProfiles("ns1").Create(ctx,
				newContainerProfile("ns1", fmt.Sprintf("cpw-%d", i), "", ""), metav1.CreateOptions{})
			time.Sleep(500 * time.Millisecond)
		}
	}()
	defer close(done)

	// The cooldown queue delays delivery by ~5s, so allow a generous timeout.
	select {
	case ev := <-eq.ResultChan:
		obj, ok := ev.Object.(metav1.Object)
		require.True(t, ok, "event object should implement metav1.Object")
		assert.Contains(t, obj.GetName(), "cpw-", "watchRetry should forward the created CP")
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for watchRetry to forward an event")
	}
}
