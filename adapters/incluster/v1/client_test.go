package incluster

import (
	"context"
	"testing"
	"time"

	"github.com/kinbiko/jsonassert"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

var (
	deploy = &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
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
				watchOpts:  metav1.ListOptions{TimeoutSeconds: ptr.To(int64(1))},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				client:        tt.fields.client,
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
		obj     *unstructured.Unstructured
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
				kind: domain.KindFromString(context.TODO(), "networking.k8s.io/v1/NetworkPolicy"),
			},
			obj:  utils.FileToUnstructured("../../../utils/testdata/networkPolicy.json"),
			want: utils.FileContent("../../../utils/testdata/networkPolicyCleaned.json"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				client:              tt.fields.client,
				account:             tt.fields.account,
				cluster:             tt.fields.cluster,
				kind:                tt.fields.kind,
				multiplier:          tt.fields.multiplier,
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
			ja := jsonassert.New(t)
			ja.Assertf(string(got), string(tt.want))
		})
	}
}
