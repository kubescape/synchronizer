package authentication

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
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
		connectionTime := time.Now()
		connectionId := uuid.New().String()

		accessKey := r.Header.Get(core.AccessKeyHeader)
		account := r.Header.Get(core.AccountHeader)
		cluster := r.Header.Get(core.ClusterNameHeader)
		helmVersion := r.Header.Get(core.HelmVersionHeader)
		version := r.Header.Get(core.VersionHeader)

		if accessKey == "" || account == "" || cluster == "" {
			logger.L().Error("missing headers on incoming connection",
				helpers.Int("accessKey (length)", len(accessKey)),
				helpers.String("account", account),
				helpers.String("cluster", cluster))

			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if version == "invalid" {
			w.WriteHeader(http.StatusFailedDependency)
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

			logger.L().Debug("creating authentication request",
				helpers.String("connId", connectionId),
				helpers.String("url", u.String()))

			authenticationRequest, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
			if err != nil {
				logger.L().Error("unable to create authentication request", helpers.Error(err))
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			for origin, dest := range cfg.HeaderToHeaderMapping {
				authenticationRequest.Header.Set(dest, r.Header.Get(origin))
			}
			logger.L().Debug("authenticating incoming connection",
				helpers.Int("accessKey (length)", len(accessKey)),
				helpers.String("account", account),
				helpers.String("cluster", cluster),
				helpers.String("connId", connectionId),
				helpers.String("url", u.String()))

			response, err := client.Do(authenticationRequest)
			if err != nil {
				logger.L().Error("authentication request failed", helpers.Error(err),
					helpers.String("account", account),
					helpers.String("cluster", cluster),
					helpers.String("connId", connectionId),
					helpers.String("url", u.String()))
				w.WriteHeader(http.StatusUnauthorized)
				return
			} else if response.StatusCode != http.StatusOK {
				logger.L().Error("authentication server did not authorize the connection",
					helpers.Int("accessKey (length)", len(accessKey)),
					helpers.String("account", account),
					helpers.String("cluster", cluster),
					helpers.String("connId", connectionId),
					helpers.Int("statusCode", response.StatusCode))
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		logger.L().Debug("connection authenticated",
			helpers.String("account", account),
			helpers.String("cluster", cluster),
			helpers.String("connId", connectionId),
			helpers.String("connectionTime", connectionTime.Format(time.RFC3339Nano)),
		)

		// create new context with client identifier
		ctx := context.WithValue(r.Context(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
			Account:        account,
			Cluster:        cluster,
			ConnectionId:   connectionId,
			ConnectionTime: connectionTime,
			HelmVersion:    helmVersion,
			Version:        version,
		})

		// create new request using the new context
		authenticatedRequest := r.WithContext(ctx)
		next.ServeHTTP(w, authenticatedRequest)
	})
}
