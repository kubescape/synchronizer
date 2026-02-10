package incluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetCloudProviderFromNode(t *testing.T) {
	tests := []struct {
		name       string
		providerID string
		expected   string
	}{
		// Standard "<provider>://" formats
		{
			name:       "AWS standard format",
			providerID: "aws:///eu-west-1a/i-099ef91944855a3fe",
			expected:   "eks",
		},
		{
			name:       "GCE standard format",
			providerID: "gce://my-project/us-central1-a/my-node",
			expected:   "gke",
		},
		{
			name:       "Azure standard format",
			providerID: "azure:///subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm",
			expected:   "aks",
		},
		{
			name:       "DigitalOcean standard format",
			providerID: "digitalocean://12345678",
			expected:   "digitalocean",
		},
		{
			name:       "OpenStack standard format",
			providerID: "openstack:///abcd-1234-efgh-5678",
			expected:   "openstack",
		},
		{
			name:       "vSphere standard format",
			providerID: "vsphere://42302e31-b314-7a61-e230-5e72c5e90942",
			expected:   "vsphere",
		},
		{
			name:       "OCI standard format with oci:// prefix",
			providerID: "oci://ocid1.instance.oc1.iad.abcd1234",
			expected:   "oracle",
		},
		{
			name:       "IBM standard format",
			providerID: "ibm://instance-id-1234",
			expected:   "ibm",
		},
		{
			name:       "Linode standard format",
			providerID: "linode://12345678",
			expected:   "linode",
		},
		// OCI non-standard format (bare OCID)
		{
			name:       "OCI bare OCID format",
			providerID: "ocid1.instance.oc1.iad.anuwcljsmlhfdnycaohqtkzfrcsioov6275acx7zqo6pth3dnjhchjauyraq",
			expected:   "oracle",
		},
		{
			name:       "OCI bare OCID uppercase",
			providerID: "OCID1.INSTANCE.OC1.IAD.ANUWCLJSMLHFDNYCAOHQTKZFRCSIOOV",
			expected:   "oracle",
		},
		// Empty / unknown
		{
			name:       "empty providerID",
			providerID: "",
			expected:   "",
		},
		{
			name:       "unknown provider",
			providerID: "somethingelse://12345",
			expected:   "",
		},
		{
			name:       "bare unknown string",
			providerID: "random-string-no-provider",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
				Spec: corev1.NodeSpec{
					ProviderID: tt.providerID,
				},
			}
			result := getCloudProviderFromNode(node)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDetectCloudProviderFromProviderID(t *testing.T) {
	tests := []struct {
		name       string
		providerID string
		expected   string
	}{
		{
			name:       "OCI bare OCID",
			providerID: "ocid1.instance.oc1.iad.anuwcljsmlhfdnycaohqtkzfrcsioov6275acx7zqo6pth3dnjhchjauyraq",
			expected:   "oracle",
		},
		{
			name:       "OCI bare OCID uppercase",
			providerID: "OCID1.INSTANCE.OC1.PHX.ABCDEF",
			expected:   "oracle",
		},
		{
			name:       "OCI bare OCID mixed case",
			providerID: "Ocid1.instance.oc1.eu-frankfurt-1.abcdef",
			expected:   "oracle",
		},
		{
			name:       "empty string",
			providerID: "",
			expected:   "",
		},
		{
			name:       "unknown format",
			providerID: "some-random-id-12345",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectCloudProviderFromProviderID(tt.providerID)
			assert.Equal(t, tt.expected, result)
		})
	}
}
