package main

import (
	"context"
	"errors"
	"net/http"
	"os"

	"github.com/go-logr/logr"
	"github.com/kubescape/k8s-interface/k8sinterface"
	"github.com/kubescape/storage/pkg/apiauth"
	httpendpoint "github.com/kubescape/synchronizer/adapters/httpendpoint/v1"
)

// newAPIAuth builds the apiauth middleware protecting the HTTP ingest endpoint.
func newAPIAuth(ctx context.Context, k8sApi *k8sinterface.KubernetesApi) (func(http.Handler) http.Handler, error) {
	cfg := apiauth.Config{Route: httpendpoint.Route, Logger: logr.Discard()}
	if os.Getenv(apiauth.DisabledEnv) == "true" {
		return apiauth.New(ctx, cfg)
	}
	cfg.Namespace = os.Getenv("POD_NAMESPACE")
	if cfg.Namespace == "" {
		return nil, errors.New("POD_NAMESPACE must be set for API auth (or set KS_API_AUTH_DISABLED=true)")
	}
	if k8sApi == nil {
		k8sApi = k8sinterface.NewKubernetesApi()
	}
	cfg.Client = k8sApi.KubernetesClient
	cfg.Audience = os.Getenv("KS_API_AUDIENCE")
	if cfg.Audience == "" {
		cfg.Audience = "kubescape.io"
	}
	return apiauth.New(ctx, cfg)
}
