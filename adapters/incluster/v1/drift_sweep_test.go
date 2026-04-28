package incluster

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// nodeUnstructured returns an unstructured node object the fake dynamic client
// can list. The fake client uses the object's GVK for routing.
func nodeUnstructured(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]interface{}{
				"name": name,
			},
		},
	}
}

// TestClient_driftSweep_emitsDeleteForOrphans is the core regression test for
// SUB-7252: when the watch has marked objects alive but they are no longer
// present in the cluster, the drift sweep must emit DELETE for exactly those
// missing objects and update the known set.
func TestClient_driftSweep_emitsDeleteForOrphans(t *testing.T) {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Version: "v1", Kind: "Node"},
		&unstructured.Unstructured{},
	)
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Version: "v1", Kind: "NodeList"},
		&unstructured.UnstructuredList{},
	)

	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	listGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NodeList"}

	// Cluster currently has node-a and node-c. node-b was deleted while the
	// synchronizer's watch was disconnected, so the watch never saw the
	// Deleted event.
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{gvr: listGVK.Kind},
		nodeUnstructured("node-a"),
		nodeUnstructured("node-c"),
	)

	var (
		mu       sync.Mutex
		deleted  []string
	)
	c := &Client{
		dynamicClient: dynClient,
		res:           gvr,
		kind:          &domain.Kind{Group: "", Version: "v1", Resource: "nodes"},
		Strategy:      domain.CopyStrategy,
		knownObjects: map[string]struct{}{
			"/node-a": {}, // alive in cluster
			"/node-b": {}, // missed delete — should be reported
			"/node-c": {}, // alive in cluster
		},
		callbacks: domain.Callbacks{
			DeleteObject: func(_ context.Context, id domain.KindName) error {
				mu.Lock()
				defer mu.Unlock()
				deleted = append(deleted, id.Name)
				return nil
			},
		},
	}

	require.NoError(t, c.driftSweep(context.Background()))

	mu.Lock()
	sort.Strings(deleted)
	mu.Unlock()
	assert.Equal(t, []string{"node-b"}, deleted, "drift sweep should delete only the missed-delete orphan")

	// Known set should now match cluster reality (node-b removed, node-a/node-c retained).
	c.knownObjectsMu.RLock()
	defer c.knownObjectsMu.RUnlock()
	_, hasA := c.knownObjects["/node-a"]
	_, hasB := c.knownObjects["/node-b"]
	_, hasC := c.knownObjects["/node-c"]
	assert.True(t, hasA)
	assert.False(t, hasB, "orphan must be removed from known set after DELETE")
	assert.True(t, hasC)
}

// TestClient_driftSweep_seedsNewClusterItems verifies that on first run the
// sweep silently learns objects already in the cluster (so that a subsequent
// disappearance is detectable) without emitting any callbacks for them.
func TestClient_driftSweep_seedsNewClusterItems(t *testing.T) {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Version: "v1", Kind: "Node"},
		&unstructured.Unstructured{},
	)
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Version: "v1", Kind: "NodeList"},
		&unstructured.UnstructuredList{},
	)

	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	listGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NodeList"}
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{gvr: listGVK.Kind},
		nodeUnstructured("node-a"),
		nodeUnstructured("node-b"),
	)

	c := &Client{
		dynamicClient: dynClient,
		res:           gvr,
		kind:          &domain.Kind{Group: "", Version: "v1", Resource: "nodes"},
		Strategy:      domain.CopyStrategy,
		knownObjects:  map[string]struct{}{},
		callbacks: domain.Callbacks{
			DeleteObject: func(_ context.Context, id domain.KindName) error {
				t.Fatalf("unexpected DeleteObject for %s on first sweep", id.Name)
				return nil
			},
		},
	}

	require.NoError(t, c.driftSweep(context.Background()))

	c.knownObjectsMu.RLock()
	defer c.knownObjectsMu.RUnlock()
	assert.Len(t, c.knownObjects, 2)
	_, hasA := c.knownObjects["/node-a"]
	_, hasB := c.knownObjects["/node-b"]
	assert.True(t, hasA)
	assert.True(t, hasB)
}

// TestClient_markKnown_markUnknown verifies the watch-loop hooks update the
// drift-sweep set correctly under concurrent access.
func TestClient_markKnown_markUnknown(t *testing.T) {
	c := &Client{knownObjects: map[string]struct{}{}}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"metadata": map[string]interface{}{"namespace": "ns", "name": "name"},
	}}

	c.markKnown(obj)
	c.knownObjectsMu.RLock()
	_, ok := c.knownObjects["ns/name"]
	c.knownObjectsMu.RUnlock()
	assert.True(t, ok, "markKnown should add ns/name to set")

	c.markUnknown(obj)
	c.knownObjectsMu.RLock()
	_, ok = c.knownObjects["ns/name"]
	c.knownObjectsMu.RUnlock()
	assert.False(t, ok, "markUnknown should remove ns/name from set")
}
