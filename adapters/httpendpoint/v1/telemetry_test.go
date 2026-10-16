package httpendpoint

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/require"
)

type forbiddenBody struct{ t *testing.T }

func (b forbiddenBody) Read([]byte) (int, error) { b.t.Fatal("invalid path read body"); return 0, nil }
func (b forbiddenBody) Close() error             { return nil }

func TestInvalidPathStopsBeforeBodyAndCallbacks(t *testing.T) {
	a := NewHTTPEndpointAdapter(config.Config{})
	a.supportedPaths = map[domain.Strategy]map[string]map[string]map[string]bool{}
	// Nil callbacks deliberately panic if reached: malformed path must exit first.
	req := httptest.NewRequest(http.MethodPost, "/otel-validation-invalid", nil)
	req.Body = forbiddenBody{t}
	req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	res := httptest.NewRecorder()
	a.ServeHTTP(res, req)
	require.Equal(t, http.StatusBadRequest, res.Code)
	require.Zero(t, a.admitted)
}
