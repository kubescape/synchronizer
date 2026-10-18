package authentication

import (
	"context"
	"io"
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

// armoAccessKeyLength is the length of a valid ARMO access key, which is a UUID.
const armoAccessKeyLength = 36

// helmPlaceholderAccounts are literal values shipped in the Helm chart / install
// docs that a customer is expected to replace with their real ARMO account UUID.
// Seeing one of these as the account means the chart was installed without
// substituting the values (e.g. `REPLACE_ME_WITH_ARMO_ACCOUNT_UUID`, or a bare
// `test`), so the connection can never authenticate no matter how many times it
// retries.
var helmPlaceholderAccounts = map[string]bool{
	"REPLACE_ME_WITH_ARMO_ACCOUNT_UUID": true,
	"test":                              true,
	"":                                  true,
}

// unconfiguredCredentialsReason returns a human-readable reason when the presented
// account/accessKey look like unsubstituted Helm placeholders rather than real
// (possibly bad or revoked) credentials. It returns an empty string when the
// credentials look plausibly real, so a genuine auth failure stays distinguishable
// from a never-configured install. It never returns or logs the raw access key.
func unconfiguredCredentialsReason(account, accessKey string) string {
	if helmPlaceholderAccounts[account] {
		return "account is a known Helm placeholder — customer likely did not substitute Helm values"
	}
	if _, err := uuid.Parse(account); err != nil {
		return "account is not a valid UUID — customer likely did not substitute Helm values"
	}
	if len(accessKey) != armoAccessKeyLength {
		return "accessKey length is not 36 (not a valid ARMO access key) — customer likely did not substitute Helm values"
	}
	return ""
}

// maxDrainBytes bounds how much of an authentication response we are willing to
// read just to let the connection be reused. The body itself is never used.
const maxDrainBytes = 4 << 10

// discardBody drains (up to maxDrainBytes) and closes an HTTP response body.
// Without this, every authenticated connection leaks one connection to the
// authentication server: the body is never closed, so the transport can neither
// reuse it nor release it. Draining first matters too, since a body that is
// closed before EOF makes the transport drop the connection instead of
// returning it to the idle pool.
func discardBody(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDrainBytes))
	_ = body.Close()
}

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
		cloudProvider := r.Header.Get(core.CloudProviderHeader)
		gitVersion := r.Header.Get(core.GitVersionHeader)
		clusterUID := r.Header.Get(core.ClusterUIDHeader)
		resourceGroup := r.Header.Get(core.ResourceGroupHeader)

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
			if err == nil {
				// Only the status code is used. Release the body now rather than
				// with a defer: this handler stays on the stack for the whole life
				// of the (long-lived) sync connection via next.ServeHTTP below.
				discardBody(response.Body)
			}
			if err != nil {
				logger.L().Error("authentication request failed", helpers.Error(err),
					helpers.String("account", account),
					helpers.String("cluster", cluster),
					helpers.String("connId", connectionId),
					helpers.String("url", u.String()))
				w.WriteHeader(http.StatusUnauthorized)
				return
			} else if response.StatusCode != http.StatusOK {
				logFields := []helpers.IDetails{
					helpers.Int("accessKey (length)", len(accessKey)),
					helpers.String("account", account),
					helpers.String("cluster", cluster),
					helpers.String("connId", connectionId),
					helpers.Int("statusCode", response.StatusCode),
				}
				// Surface obviously-unconfigured credentials (unsubstituted Helm
				// placeholders) so support can tell them apart from a genuine
				// bad/revoked key without changing the 401 behavior itself.
				if reason := unconfiguredCredentialsReason(account, accessKey); reason != "" {
					logFields = append(logFields, helpers.String("reason", reason))
				}
				logger.L().Error("authentication server did not authorize the connection", logFields...)
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
			SyncVersion:    version,
			CloudProvider:  cloudProvider,
			GitVersion:     gitVersion,
			ClusterUID:     clusterUID,
			ResourceGroup:  resourceGroup,
		})
		ctx = context.WithValue(ctx, domain.ContextKeyAccessKey, accessKey)

		// create new request using the new context
		authenticatedRequest := r.WithContext(ctx)
		next.ServeHTTP(w, authenticatedRequest)
	})
}
