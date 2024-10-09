package incluster

import (
	"context"

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
	nodeList, err := k8sApi.KubernetesClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
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
