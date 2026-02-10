package incluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"

	cloudsupportv1 "github.com/kubescape/k8s-interface/cloudsupport/v1"
	"github.com/kubescape/k8s-interface/k8sinterface"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func GetApiServerGitVersionAndCloudProvider(ctx context.Context) (string, string) {
	k8sAPiObj := k8sinterface.NewKubernetesApi()

	cloudProvider, err := getCloudProvider(ctx, k8sAPiObj)
	if err != nil {
		logger.L().Error("failed to set cloud provider", helpers.Error(err))
	} else {
		logger.L().Info("cloud provider", helpers.String("cloudProvider", cloudProvider))
	}

	gitVersion, err := getApiServerGitVersion(k8sAPiObj)
	if err != nil {
		logger.L().Error("failed to get api server version", helpers.Error(err))
	} else {
		logger.L().Info("cluster api server", helpers.String("GitVersion", gitVersion))
	}

	return gitVersion, cloudProvider
}

func getCloudProvider(ctx context.Context, k8sApi *k8sinterface.KubernetesApi) (string, error) {
	// Fetch only a single node to extract the providerID, instead of listing all nodes.
	// The providerID is the most reliable way to determine the cloud provider.
	nodeList, err := k8sApi.KubernetesClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}
	if len(nodeList.Items) == 0 {
		return "", fmt.Errorf("no nodes found in the cluster")
	}
	return getCloudProviderFromNode(&nodeList.Items[0]), nil
}

// getCloudProviderFromNode determines the cloud provider from a node's spec.providerID.
// It first tries the standard "<provider>://<details>" parsing (e.g. "aws://...", "gce://...", "azure://...").
// If that doesn't match, it falls back to content-based detection for providers that use
// non-standard providerID formats (e.g. OCI uses bare OCIDs like "ocid1.instance.oc1.iad.<id>").
func getCloudProviderFromNode(node *corev1.Node) string {
	// Try the standard "<provider>://" format first
	if provider := cloudsupportv1.GetCloudProviderFromNode(node); provider != "" {
		return provider
	}

	// Fallback: detect provider from non-standard providerID formats
	return detectCloudProviderFromProviderID(node.Spec.ProviderID)
}

// detectCloudProviderFromProviderID handles providerID values that don't follow
// the standard "<provider>://" convention. Add new cases here as they are discovered.
func detectCloudProviderFromProviderID(providerID string) string {
	if providerID == "" {
		return ""
	}

	lowerID := strings.ToLower(providerID)
	switch {
	case strings.Contains(lowerID, "ocid"):
		return cloudsupportv1.Oracle
	}

	return ""
}

func getApiServerGitVersion(k8sApi *k8sinterface.KubernetesApi) (string, error) {
	serverVersion, err := k8sApi.KubernetesClient.Discovery().ServerVersion()
	if err != nil {
		return "Unknown", err
	}

	return serverVersion.GitVersion, nil
}
