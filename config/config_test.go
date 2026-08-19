package config

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/armosec/armoapi-go/armotypes"
	"github.com/armosec/utils-k8s-go/armometadata"
	v3 "github.com/kubescape/backend/pkg/servicediscovery/v3"
	pulsarconfig "github.com/kubescape/messaging/pulsar/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func TestLoadClusterConfig(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want armometadata.ClusterConfig
	}{
		{
			name: "cluster config",
			env:  map[string]string{"CLUSTER_CONFIG": "../configuration/clusterData.json"},
			want: armometadata.ClusterConfig{
				ClusterName:         "kind",
				GatewayWebsocketURL: "gateway:8001",
				GatewayRestURL:      "gateway:8002",
				KubevulnURL:         "kubevuln:8080",
				KubescapeURL:        "kubescape:8080",
				InstallationData: armotypes.InstallationData{
					StorageEnabled:                            ptr.To[bool](true),
					RelevantImageVulnerabilitiesEnabled:       ptr.To[bool](false),
					RelevantImageVulnerabilitiesConfiguration: "disable",
					Namespace:                           "kubescape",
					ImageVulnerabilitiesScanningEnabled: ptr.To[bool](false),
					PostureScanEnabled:                  ptr.To[bool](false),
					OtelCollectorEnabled:                ptr.To[bool](true),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				err := os.Setenv(k, v)
				assert.NoError(t, err)
			}
			got, err := LoadClusterConfig()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name string
		path string
		want Config
	}{
		{
			name: "client config",
			path: "../configuration/client",
			want: Config{
				InCluster: InCluster{
					ServerUrl:         "ws://127.0.0.1:8080/",
					Namespace:         "kubescape",
					ClusterName:       "cluster-1",
					ExcludeNamespaces: []string{"kube-system", "kubescape"},
					IncludeNamespaces: []string{},
					Account:           "11111111-2222-3333-4444-11111111",
					AccessKey:         "xxxxxxxx-1111-1111-1111-xxxxxxxx",
					Resources: []Resource{
						{Group: "", Version: "v1", Resource: "pods", Strategy: "patch"},
						{Group: "", Version: "v1", Resource: "nodes", Strategy: "patch"},
						{Group: "apps", Version: "v1", Resource: "deployments", Strategy: "patch"},
						{Group: "apps", Version: "v1", Resource: "statefulsets", Strategy: "patch"},
						{Group: "spdx.softwarecomposition.kubescape.io", Version: "v1beta1", Resource: "containerprofiles", Strategy: "patch"},
					},
				},
				HTTPEndpoint: HTTPEndpoint{
					ServerPort: "8089",
					Resources: []Resource{
						{Group: "test-ks", Version: "v1", Resource: "alerts", Strategy: "copy"},
					},
				},
			},
		},
		{
			name: "server config",
			path: "../configuration/server",
			want: Config{
				Backend: Backend{
					AuthenticationServer: nil,
					Subscription:         "synchronizer-server",
					PulsarConfig: &pulsarconfig.PulsarConfig{
						URL:                    "pulsar://localhost:6650",
						Tenant:                 "armo",
						Namespace:              "kubescape",
						AdminUrl:               "http://localhost:8081",
						Clusters:               []string{"standalone"},
						RedeliveryDelaySeconds: 5,
						MaxDeliveryAttempts:    20,
					},
					ProducerTopic:  "synchronizer",
					ConsumerTopic:  "synchronizer",
					SkipAlertsFrom: []string{"foo", "bar"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadConfig(tt.path)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadServiceURLs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"version": "v3",
			"response": map[string]string{
				"event-receiver-http": "https://er-test.com",
				"api-server":          "https://api.test.com",
				"metrics":             "https://metrics.test.com",
				"synchronizer":        "ws://127.0.0.1:8080",
			},
		})
	}))
	defer srv.Close()

	got, err := LoadServiceURLs(srv.URL)
	assert.NoError(t, err)
	assert.Equal(t, &v3.ServicesV3{
		EventReceiverHttpUrl: "https://er-test.com",
		ApiServerUrl:         "https://api.test.com",
		MetricsUrl:           "https://metrics.test.com",
		SynchronizerUrl:      "ws://127.0.0.1:8080",
	}, got)
}

func TestResource_String(t *testing.T) {
	type fields struct {
		Group    string
		Version  string
		Resource string
		Strategy domain.Strategy
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "deployments",
			fields: fields{
				Group:    "apps",
				Version:  "v1",
				Resource: "deployments",
			},
			want: "apps/v1/deployments",
		},
		{
			name: "pods",
			fields: fields{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			},
			want: "/v1/pods",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Resource{
				Group:    tt.fields.Group,
				Version:  tt.fields.Version,
				Resource: tt.fields.Resource,
				Strategy: tt.fields.Strategy,
			}
			if got := r.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// a temp config rather than configuration/server-kafka, where this is false anyway: that
// matches the zero value too, so a misspelled mapstructure tag would still pass
func TestLoadConfig_KafkaRequireTopicsExist(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"),
		[]byte(`{"backend":{"messageQueue":{"type":"kafka","kafkaConfig":{"requireTopicsExist":true}}}}`), 0o600))

	config, err := LoadConfig(dir)
	require.NoError(t, err)
	require.NotNil(t, config.Backend.MessageQueue)
	require.NotNil(t, config.Backend.MessageQueue.KafkaConfig)
	assert.True(t, config.Backend.MessageQueue.KafkaConfig.RequireTopicsExist)
}
