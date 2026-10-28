package authentication

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An authentication server that fails says nothing about the credentials, so the client
// is told to retry; a refusal stays a 401, which the client treats as final.
func TestMiddleware_AuthServerFailuresAreRetriedAndRefusalsAreNot(t *testing.T) {
	for upstream, want := range map[int]int{
		http.StatusInternalServerError: http.StatusServiceUnavailable,
		http.StatusBadGateway:          http.StatusServiceUnavailable,
		http.StatusServiceUnavailable:  http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:      http.StatusServiceUnavailable,
		http.StatusUnauthorized:        http.StatusUnauthorized,
		http.StatusForbidden:           http.StatusUnauthorized,
	} {
		nextCalled := false
		h, _ := setup(t, &config.AuthenticationServerConfig{Url: "http://auth.invalid/check"},
			func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: upstream, Body: io.NopCloser(strings.NewReader(""))}, nil
			},
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) { nextCalled = true }))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, validRequest())
		assert.Equal(t, want, rec.Code, "upstream %d", upstream)
		assert.False(t, nextCalled, "upstream %d", upstream)
	}
}

// The query the mapping builds carries the access key; neither the debug lines nor the
// transport error, whose text repeats the URL, may put it in the log.
func TestMiddleware_KeepsTheAccessKeyOutOfItsLogs(t *testing.T) {
	logger.InitLogger("pretty")
	logFile, err := os.Create(filepath.Join(t.TempDir(), "server.log"))
	require.NoError(t, err)
	defer logFile.Close()
	logger.L().SetWriter(logFile)
	defer logger.L().SetWriter(os.Stderr)
	require.NoError(t, logger.L().SetLevel("debug"))

	cfg := &config.AuthenticationServerConfig{
		Url:                       "http://auth.invalid/check",
		HeaderToQueryParamMapping: map[string]string{core.AccessKeyHeader: "apiKey"},
	}
	h, _ := setup(t, cfg, func(r *http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Get", URL: r.URL.String(), Err: errors.New("connection refused")}
	}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	rec := httptest.NewRecorder()
	req := validRequest()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	logged, err := os.ReadFile(logFile.Name())
	require.NoError(t, err)
	assert.Contains(t, string(logged), "connection refused")
	assert.Contains(t, string(logged), "http://auth.invalid/check")
	assert.NotContains(t, string(logged), req.Header.Get(core.AccessKeyHeader))
	assert.NotContains(t, string(logged), "apiKey=")
}
