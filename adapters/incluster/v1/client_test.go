package incluster

import (
	"context"
	"testing"
	"time"

	"github.com/kubescape/storage/pkg/apis/softwarecomposition/v1beta1"
	storagefake "github.com/kubescape/storage/pkg/generated/clientset/versioned/fake"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

var (
	deploy = &appsv1.Deployment{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       "test",
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "test"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
				},
			},
		},
	}
)

func TestClient_watchRetry(t *testing.T) {
	type fields struct {
		client        dynamic.Interface
		account       string
		cluster       string
		kind          *domain.Kind
		callbacks     domain.Callbacks
		res           schema.GroupVersionResource
		ShadowObjects map[string][]byte
		Strategy      domain.Strategy
	}
	type args struct {
		ctx        context.Context
		watchOpts  metav1.ListOptions
		eventQueue *utils.CooldownQueue
	}
	// we need a real client to test this, as the fake client ignores opts
	ctx := context.TODO()
	k3sC, err := k3s.RunContainer(ctx,
		testcontainers.WithImage("docker.io/rancher/k3s:v1.27.9-k3s1"),
	)
	defer func() {
		_ = k3sC.Terminate(ctx)
	}()
	require.NoError(t, err)
	kubeConfigYaml, err := k3sC.GetKubeConfig(ctx)
	require.NoError(t, err)
	clusterConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeConfigYaml)
	require.NoError(t, err)
	dynamicClient := dynamic.NewForConfigOrDie(clusterConfig)
	k8sclient := kubernetes.NewForConfigOrDie(clusterConfig)
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test reconnect after timeout",
			fields: fields{
				client: dynamicClient,
				res:    schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
			},
			args: args{
				eventQueue: utils.NewCooldownQueue(),
				watchOpts:  metav1.ListOptions{TimeoutSeconds: new(int64(1))},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				dynamicClient: tt.fields.client,
				account:       tt.fields.account,
				cluster:       tt.fields.cluster,
				kind:          tt.fields.kind,
				callbacks:     tt.fields.callbacks,
				res:           tt.fields.res,
				ShadowObjects: tt.fields.ShadowObjects,
				Strategy:      tt.fields.Strategy,
			}
			go c.watchRetry(ctx, tt.args.watchOpts, tt.args.eventQueue)
			time.Sleep(5 * time.Second)
			_, err = k8sclient.AppsV1().Deployments("default").Create(context.TODO(), deploy, metav1.CreateOptions{})
			require.NoError(t, err)
			time.Sleep(1 * time.Second)
			var found bool
			for event := range tt.args.eventQueue.ResultChan {
				if event.Type == watch.Modified && event.Object.(*unstructured.Unstructured).GetName() == "test" {
					found = true
					break
				}
			}
			assert.True(t, found)
		})
	}
}

func TestClient_filterAndMarshal(t *testing.T) {
	type fields struct {
		client              dynamic.Interface
		account             string
		cluster             string
		kind                *domain.Kind
		multiplier          int
		callbacks           domain.Callbacks
		res                 schema.GroupVersionResource
		ShadowObjects       map[string][]byte
		Strategy            domain.Strategy
		batchProcessingFunc map[domain.BatchType]BatchProcessingFunc
	}
	tests := []struct {
		name    string
		fields  fields
		obj     metav1.Object
		want    []byte
		wantErr bool
	}{
		{
			name: "filter pod (no modifications)",
			fields: fields{
				kind: domain.KindFromString(context.TODO(), "/v1/pods"),
			},
			obj:  utils.FileToUnstructured("../../../utils/testdata/pod.json"),
			want: utils.FileContent("../../../utils/testdata/pod.json"),
		},
		{
			name: "filter node",
			fields: fields{
				kind: domain.KindFromString(context.TODO(), "/v1/nodes"),
			},
			obj:  utils.FileToUnstructured("../../../utils/testdata/node.json"),
			want: utils.FileContent("testdata/nodeFiltered.json"),
		},
		{
			name: "filter networkPolicy",
			fields: fields{
				kind: domain.KindFromString(context.TODO(), "networking.k8s.io/v1/networkpolicies"),
			},
			obj:  utils.FileToUnstructured("../../../utils/testdata/networkPolicy.json"),
			want: utils.FileContent("../../../utils/testdata/networkPolicyCleaned.json"),
		},
		{
			name: "filter sbomsyft",
			fields: fields{
				kind: domain.KindFromString(context.TODO(), "spdx.softwarecomposition.kubescape.io/v1beta1/sbomsyfts"),
			},
			obj:  utils.FileToUnstructured("../../../utils/testdata/sbomsyft.json"),
			want: utils.FileContent("../../../utils/testdata/sbomsyftCleaned.json"),
		},
		{
			name: "filter containerProfile",
			fields: fields{
				kind: domain.KindFromString(context.TODO(), "spdx.softwarecomposition.kubescape.io/v1beta1/containerprofiles"),
			},
			obj: &v1beta1.ContainerProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
				},
				Spec: v1beta1.ContainerProfileSpec{
					LabelSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "test"},
					},
					Egress: []v1beta1.NetworkNeighbor{
						{
							Identifier: "e5e8ca3d76f701a19b7478fdc1c8c24ccc6cef9902b52c8c7e015439e2a1ddf3",
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"kubernetes.io/metadata.name:": "kube-system"},
							},
							PodSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"k8s-app:": "kube-dns"},
							},
							Ports: []v1beta1.NetworkPort{
								{Name: "UDP-53", Protocol: "UDP", Port: ptr.To[int32](53)},
								{Name: "TCP-53", Protocol: "TCP", Port: ptr.To[int32](53)},
							},
							Type: "internal",
						},
					},
				},
			},
			want: utils.FileContent("testdata/containerProfileFiltered.json"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				dynamicClient:       tt.fields.client,
				account:             tt.fields.account,
				cluster:             tt.fields.cluster,
				kind:                tt.fields.kind,
				callbacks:           tt.fields.callbacks,
				res:                 tt.fields.res,
				ShadowObjects:       tt.fields.ShadowObjects,
				Strategy:            tt.fields.Strategy,
				batchProcessingFunc: tt.fields.batchProcessingFunc,
			}
			got, err := c.filterAndMarshal(tt.obj)
			if (err != nil) != tt.wantErr {
				t.Errorf("filterAndMarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.JSONEq(t, string(got), string(tt.want))
		})
	}
}

// TestClient_GetObject_TypedSBOMSyftFieldsRemoved checks that the sbomsyfts fields listed
// in fieldsToRemove are also removed on the path production uses: getResource fetches
// sbomsyfts with the typed storage client, so filterAndMarshal gets a *v1beta1.SBOMSyft,
// not the *unstructured.Unstructured that TestClient_filterAndMarshal passes in.
func TestClient_GetObject_TypedSBOMSyftFieldsRemoved(t *testing.T) {
	sbom := &v1beta1.SBOMSyft{
		ObjectMeta: metav1.ObjectMeta{Name: "nginx-1234", Namespace: "kubescape", ResourceVersion: "1"},
		Spec: v1beta1.SBOMSyftSpec{
			Syft: v1beta1.SyftDocument{
				Artifacts: []v1beta1.SyftPackage{
					{PackageBasicData: v1beta1.PackageBasicData{ID: "pkg", Name: "openssl", Version: "3.0.2"}},
				},
				ArtifactRelationships: []v1beta1.SyftRelationship{{Parent: "pkg", Child: "file", Type: "contains"}},
				Files:                 []v1beta1.SyftFile{{ID: "file"}},
				SyftDescriptor:        v1beta1.SyftDescriptor{Name: "syft", Version: "v1.18.1"},
			},
		},
	}
	cs := storagefake.NewSimpleClientset(sbom) //nolint:staticcheck // SA1019: same fake as the ContainerProfile watch test
	c := &Client{
		storageClient: cs.SpdxV1beta1(),
		dynamicClient: fake.NewSimpleDynamicClient(runtime.NewScheme()),
		kind:          domain.KindFromString(context.TODO(), "spdx.softwarecomposition.kubescape.io/v1beta1/sbomsyfts"),
		res:           schema.GroupVersionResource{Group: kubescapeCustomResourceGroup, Version: "v1beta1", Resource: "sbomsyfts"},
		ShadowObjects: map[string][]byte{},
		Strategy:      domain.CopyStrategy,
	}
	// the object really comes from the typed client
	obj, err := c.getResource("kubescape", "nginx-1234")
	require.NoError(t, err)
	require.IsType(t, &v1beta1.SBOMSyft{}, obj)

	var sent []byte
	c.callbacks = domain.Callbacks{
		PutObject: func(_ context.Context, _ domain.KindName, _ string, object []byte) error {
			sent = object
			return nil
		},
	}
	require.NoError(t, c.GetObject(context.TODO(), domain.KindName{Kind: c.kind, Name: "nginx-1234", Namespace: "kubescape"}, nil))
	require.NotNil(t, sent, "GetObject should send the object")

	var got unstructured.Unstructured
	require.NoError(t, got.UnmarshalJSON(sent))
	syft, found, err := unstructured.NestedMap(got.Object, "spec", "syft")
	require.NoError(t, err)
	require.True(t, found)
	for _, field := range []string{"descriptor", "files", "artifactRelationships"} {
		assert.Nil(t, syft[field], "spec.syft.%s should be removed before sending", field)
	}
	assert.NotEmpty(t, syft["artifacts"], "the packages should be kept")
	assert.Equal(t, "SBOMSyft", got.GetKind())
	assert.Equal(t, "spdx.softwarecomposition.kubescape.io/v1beta1", got.GetAPIVersion())
}

// TestClient_GetObject_TypedSBOMSyftNumbersKept: stripping fields from a typed object must
// not change the numbers that stay. Package metadata is raw JSON, so it can hold integers
// beyond 2^53 or beyond int64 and decimals that a float64 round trip would rewrite.
func TestClient_GetObject_TypedSBOMSyftNumbersKept(t *testing.T) {
	metadata := `{"installedSize":9007199254740993,"maxSize":18446744073709551615,"ratio":0.10000000000000000555}`
	sbom := &v1beta1.SBOMSyft{
		ObjectMeta: metav1.ObjectMeta{Name: "nginx-1234", Namespace: "kubescape", ResourceVersion: "1"},
		Spec: v1beta1.SBOMSyftSpec{
			Syft: v1beta1.SyftDocument{
				Artifacts: []v1beta1.SyftPackage{{
					PackageBasicData:  v1beta1.PackageBasicData{ID: "pkg", Name: "openssl", Version: "3.0.2"},
					PackageCustomData: v1beta1.PackageCustomData{MetadataType: "dpkg-db-entry", Metadata: []byte(metadata)},
				}},
				Files: []v1beta1.SyftFile{{ID: "file"}},
			},
		},
	}
	cs := storagefake.NewSimpleClientset(sbom) //nolint:staticcheck // SA1019: same fake as the ContainerProfile watch test
	c := &Client{
		storageClient: cs.SpdxV1beta1(),
		dynamicClient: fake.NewSimpleDynamicClient(runtime.NewScheme()),
		kind:          domain.KindFromString(context.TODO(), "spdx.softwarecomposition.kubescape.io/v1beta1/sbomsyfts"),
		res:           schema.GroupVersionResource{Group: kubescapeCustomResourceGroup, Version: "v1beta1", Resource: "sbomsyfts"},
		ShadowObjects: map[string][]byte{},
		Strategy:      domain.CopyStrategy,
	}
	var sent []byte
	c.callbacks = domain.Callbacks{
		PutObject: func(_ context.Context, _ domain.KindName, _ string, object []byte) error {
			sent = object
			return nil
		},
	}
	require.NoError(t, c.GetObject(context.TODO(), domain.KindName{Kind: c.kind, Name: "nginx-1234", Namespace: "kubescape"}, nil))
	require.NotNil(t, sent, "GetObject should send the object")
	for _, number := range []string{"9007199254740993", "18446744073709551615", "0.10000000000000000555"} {
		assert.Contains(t, string(sent), number, "the number should be sent unchanged")
	}
	assert.NotContains(t, string(sent), `{"id":"file"`, "spec.syft.files should still be removed")
}

func TestClient_isFiltered(t *testing.T) {
	tests := []struct {
		name     string
		workload metav1.Object
		filtered bool
	}{
		{
			name:     "nil workload",
			filtered: false,
		},
		{
			name: "pod",
			workload: &unstructured.Unstructured{Object: map[string]any{
				"kind":     "Pod",
				"metadata": map[string]any{"namespace": "default"}}},
			filtered: false,
		},
		{
			name: "pod with ownerReferences",
			workload: &unstructured.Unstructured{Object: map[string]any{
				"kind": "Pod",
				"metadata": map[string]any{
					"ownerReferences": []any{map[string]any{
						"apiVersion": "batch/v1"}}}}},
			filtered: true,
		},
		{
			name: "pod with pod-template-hash",
			workload: &unstructured.Unstructured{Object: map[string]any{
				"kind": "Pod",
				"metadata": map[string]any{
					"labels": map[string]any{
						"pod-template-hash": "12345"}}}},
			filtered: true,
		},
		{
			name: "pod from kubescape namespace", // special case, never filter out
			workload: &unstructured.Unstructured{Object: map[string]any{
				"kind":     "Pod",
				"metadata": map[string]any{"namespace": "default"}}},
			filtered: false,
		},
		{
			name: "pod from filtered namespace",
			workload: &unstructured.Unstructured{Object: map[string]any{
				"kind":     "Pod",
				"metadata": map[string]any{"namespace": "kube-system"}}},
			filtered: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				excludeNamespaces: []string{"kube-system", "kubescape"},
				includeNamespaces: []string{},
				operatorNamespace: "kubescape",
			}
			assert.Equalf(t, tt.filtered, c.isFiltered(tt.workload), "isFiltered(%v)", tt.workload)
		})
	}
}

func Test_mergeMetadata(t *testing.T) {
	existingDeploy := &appsv1.Deployment{
		Name:        "test",
		Namespace:   "default",
		Labels:      map[string]string{"app": "test"},
		Annotations: map[string]string{"deployment.kubernetes.io/revision": "2"},
	}
	newDeploy := &appsv1.Deployment{
		Name:        "test",
		Namespace:   "default",
		Labels:      map[string]string{"app": "test"},
		Annotations: map[string]string{"action-guid": "4e4f9032-d03f-459f-b280-17125c50f88b", "deployment.kubernetes.io/revision": "1"},
	}
	existingUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(existingDeploy)
	require.NoError(t, err)
	newUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(newDeploy)
	require.NoError(t, err)
	type args struct {
		existing map[string]any
		new      map[string]any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test merge metadata",
			args: args{
				existing: existingUnstructured["metadata"].(map[string]any),
				new:      newUnstructured["metadata"].(map[string]any),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mergeMetadata(tt.args.existing, tt.args.new)
			assert.EqualValues(t, map[string]any{"action-guid": "4e4f9032-d03f-459f-b280-17125c50f88b", "deployment.kubernetes.io/revision": "2"}, tt.args.existing["annotations"])
		})
	}
}

func TestReconcileBatchProcessingFunc_NilKind(t *testing.T) {
	ctx := context.Background()
	resource := config.Resource{Group: "apps", Version: "v1", Resource: "deployments", Strategy: domain.CopyStrategy}

	existingObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":            "existing-sample",
				"namespace":       "default",
				"resourceVersion": "1",
			},
		},
	}
	existingMatchingObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":            "existing-matching",
				"namespace":       "default",
				"resourceVersion": "3",
			},
		},
	}

	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), existingObj, existingMatchingObj)
	c := NewClient(dynamicClient, nil, config.InCluster{Namespace: "kubescape"}, resource)

	var deletedIDs []domain.KindName
	var putIDs []domain.KindName
	c.RegisterCallbacks(ctx, domain.Callbacks{
		DeleteObject: func(ctx context.Context, id domain.KindName) error {
			deletedIDs = append(deletedIDs, id)
			return nil
		},
		PutObject: func(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
			putIDs = append(putIDs, id)
			return nil
		},
	})

	// When receiving a reconciliation batch from the server, individual items
	// have Kind set to nil. This tests non-existing resources (delete), existing resources with
	// matching version (skip), and existing resources with changed version (put).
	items := domain.BatchItems{
		NewChecksum: []domain.NewChecksum{
			{
				Kind:            nil,
				Namespace:       "default",
				Name:            "non-existing",
				ResourceVersion: 1,
				Checksum:        "abc",
			},
			{
				Kind:            nil,
				Namespace:       "default",
				Name:            "existing-sample",
				ResourceVersion: 2, // version mismatch (2 != 1) -> should trigger put
				Checksum:        "def",
			},
			{
				Kind:            nil,
				Namespace:       "default",
				Name:            "existing-matching",
				ResourceVersion: 3, // version match (3 == 3) -> should skip
				Checksum:        "ghi",
			},
		},
	}

	err := c.Batch(ctx, *c.kind, domain.ReconciliationBatch, items)
	require.NoError(t, err)

	require.Len(t, deletedIDs, 1)
	assert.Equal(t, "non-existing", deletedIDs[0].Name)
	assert.Equal(t, "default", deletedIDs[0].Namespace)
	assert.Equal(t, c.kind.String(), deletedIDs[0].Kind.String())

	require.Len(t, putIDs, 1)
	assert.Equal(t, "existing-sample", putIDs[0].Name)
	assert.Equal(t, "default", putIDs[0].Namespace)
	assert.Equal(t, c.kind.String(), putIDs[0].Kind.String())
}

func TestAdapter_GetClientByKind_UnconfiguredKind_NoOp(t *testing.T) {
	ctx := context.Background()
	a := NewInClusterAdapter(config.InCluster{Namespace: "kubescape"}, nil, nil)

	var calls int
	a.RegisterCallbacks(ctx, domain.Callbacks{
		DeleteObject: func(ctx context.Context, id domain.KindName) error { calls++; return nil },
		GetObject:    func(ctx context.Context, id domain.KindName, _ []byte) error { calls++; return nil },
		PutObject:    func(ctx context.Context, id domain.KindName, _ string, _ []byte) error { calls++; return nil },
		PatchObject:  func(ctx context.Context, id domain.KindName, _ string, _ []byte) error { calls++; return nil },
		VerifyObject: func(ctx context.Context, id domain.KindName, _ string) error { calls++; return nil },
	})

	kind := domain.Kind{Group: "spdx.softwarecomposition.kubescape.io", Version: "v1beta1", Resource: "applicationprofiles"}
	client := a.GetClientByKind(kind)
	require.NotNil(t, client)

	id := domain.KindName{Kind: &kind, Namespace: "default", Name: "app-1", ResourceVersion: 1}

	// Batch message for unconfigured resource should be silently discarded and not trigger reconciliation or callbacks
	items := domain.BatchItems{
		NewChecksum: []domain.NewChecksum{
			{
				Namespace:       "default",
				Name:            "app-1",
				ResourceVersion: 1,
				Checksum:        "abc",
			},
		},
	}
	require.NoError(t, client.Batch(ctx, kind, domain.ReconciliationBatch, items))
	require.NoError(t, client.Batch(ctx, kind, domain.DefaultBatch, items))
	require.NoError(t, client.DeleteObject(ctx, id))
	require.NoError(t, client.PutObject(ctx, id, "abc", []byte(`{}`)))
	require.NoError(t, client.VerifyObject(ctx, id, "abc"))
	require.NoError(t, client.GetObject(ctx, id, nil))
	require.NoError(t, client.PatchObject(ctx, id, "abc", []byte(`{}`)))
	require.NoError(t, client.Start(ctx))
	require.Equal(t, 0, calls)
}


