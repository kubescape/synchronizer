package authentication

import (
	"context"
	"net/http"
	"net/url"
	"sync"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
)

var (
	client *http.Client
	once   sync.Once // used to initialize authHttpClient
)

func AuthenticationServerMiddleware(cfg *config.AuthenticationServerConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() {
			if cfg == nil || cfg.Url == "" {
				logger.L().Warning("authentication server is not set; Incoming connections will not be authenticated")
			} else {
				client = &http.Client{}
			}
		})

		accessKey := r.Header.Get(core.AccessKeyHeader)
		account := r.Header.Get(core.AccountHeader)
		cluster := r.Header.Get(core.ClusterNameHeader)

		if accessKey == "" || account == "" || cluster == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if client != nil {
			u, err := url.Parse(cfg.Url)
			if err != nil {
				panic(err)
			}

			// copy headers to authentication request query params (configurable)
			q := u.Query()
			for header, queryParam := range cfg.HeaderToQueryParamMapping {
				q.Set(queryParam, r.Header.Get(header))
			}
			u.RawQuery = q.Encode()

			authenticationRequest, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
			if err != nil {
				logger.L().Error("unable to create authentication request", helpers.Error(err))
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			for origin, dest := range cfg.HeaderToHeaderMapping {
				authenticationRequest.Header.Set(dest, r.Header.Get(origin))
			}

			response, err := client.Do(authenticationRequest)
			if err != nil || response.StatusCode != http.StatusOK {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		// create new context with client identifier
		ctx := context.WithValue(r.Context(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
			Account: account,
			Cluster: cluster,
		})
		// create new request using the new context
		authenticatedRequest := r.WithContext(ctx)
		next.ServeHTTP(w, authenticatedRequest)
	})
}
