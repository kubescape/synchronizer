package incluster

import (
	"context"
	"fmt"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"

	clouds "github.com/kubescape/k8s-interface/cloudsupport"
	"github.com/kubescape/k8s-interface/k8sinterface"
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
	nodeList, err := k8sApi.KubernetesClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}
	if len(nodeList.Items) == 0 {
		return "", fmt.Errorf("no nodes found in the cluster")
	}
	return clouds.GetCloudProvider(nodeList), nil
}

func getApiServerGitVersion(k8sApi *k8sinterface.KubernetesApi) (string, error) {
	serverVersion, err := k8sApi.KubernetesClient.Discovery().ServerVersion()
	if err != nil {
		return "Unknown", err
	}

	return serverVersion.GitVersion, nil
}
