package config

import (
	"os"
	"testing"

	"github.com/armosec/armoapi-go/armotypes"
	"github.com/armosec/utils-k8s-go/armometadata"
	"github.com/kubescape/backend/pkg/servicediscovery/schema"
	v2 "github.com/kubescape/backend/pkg/servicediscovery/v2"
	pulsarconfig "github.com/kubescape/messaging/pulsar/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
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
					ServerUrl:   "ws://127.0.0.1:8080/",
					ClusterName: "cluster-1",
					Account:     "11111111-2222-3333-4444-11111111",
					AccessKey:   "xxxxxxxx-1111-1111-1111-xxxxxxxx",
					Resources: []Resource{
						{Group: "apps", Version: "v1", Resource: "deployments", Strategy: "patch"},
						{Group: "apps", Version: "v1", Resource: "statefulsets", Strategy: "patch"},
						{Group: "spdx.softwarecomposition.kubescape.io", Version: "v1beta1", Resource: "applicationprofiles", Strategy: "patch"},
					},
				},
			},
		},
		{
			name: "server config",
			path: "../configuration/server",
			want: Config{
				Backend: Backend{
					AuthenticationServer: &AuthenticationServerConfig{
						Url:                       "https://api.armosec.io/api/v1",
						HeaderToQueryParamMapping: map[string]string{"x-api-account": "customerGUID"},
						HeaderToHeaderMapping:     map[string]string{"x-api-key": "X-API-KEY"},
					},
					Subscription: "synchronizer-server",
					PulsarConfig: &pulsarconfig.PulsarConfig{
						URL:                    "pulsar://localhost:6650",
						Tenant:                 "armo",
						Namespace:              "kubescape",
						AdminUrl:               "http://localhost:8080",
						Clusters:               []string{"standalone"},
						RedeliveryDelaySeconds: 5,
						MaxDeliveryAttempts:    20,
					},
					Topic: "synchronizer",
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
	tests := []struct {
		name     string
		env      map[string]string
		filePath string
		want     schema.IBackendServices
	}{
		{
			name:     "via filePath",
			filePath: "../configuration/services.json",
			want: &v2.ServicesV2{
				EventReceiverHttpUrl:      "https://er-test.com",
				EventReceiverWebsocketUrl: "wss://er-test.com",
				GatewayUrl:                "https://gw.test.com",
				ApiServerUrl:              "https://api.test.com",
				MetricsUrl:                "https://metrics.test.com",
				SynchronizerUrl:           "wss://synchronizer.test.com",
			},
		},
		{
			name: "via env",
			env:  map[string]string{"SERVICES": "../configuration/services.json"},
			want: &v2.ServicesV2{
				EventReceiverHttpUrl:      "https://er-test.com",
				EventReceiverWebsocketUrl: "wss://er-test.com",
				GatewayUrl:                "https://gw.test.com",
				ApiServerUrl:              "https://api.test.com",
				MetricsUrl:                "https://metrics.test.com",
				SynchronizerUrl:           "wss://synchronizer.test.com",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				err := os.Setenv(k, v)
				assert.NoError(t, err)
			}
			got, err := LoadServiceURLs(tt.filePath)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
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
