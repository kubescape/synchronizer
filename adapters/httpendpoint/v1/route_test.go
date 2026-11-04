package httpendpoint

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kubescape/storage/pkg/apiauth"
	"github.com/kubescape/synchronizer/config"
	"github.com/stretchr/testify/require"
)

func TestRoute_ParsesGroupAndResource(t *testing.T) {
	for _, m := range []string{http.MethodPost, http.MethodGet} {
		attrs, ok := Route(httptest.NewRequest(m, "/apis/v1/kubescape.io/v1/runtimealerts", nil))
		require.True(t, ok)
		require.Equal(t, apiauth.Attributes{Group: "kubescape.io", Resource: "runtimealerts", Verb: "create"}, attrs)
	}
}

func TestRoute_ProbesOpen(t *testing.T) {
	for _, p := range []string{"/healthz", "/readyz"} {
		attrs, ok := Route(httptest.NewRequest(http.MethodGet, p, nil))
		require.False(t, ok)
		require.Equal(t, apiauth.Attributes{}, attrs)
	}
}

func TestRoute_UnknownPathProtected(t *testing.T) {
	for _, p := range []string{"/", "/foo", "/apis/v1/kubescape.io/v1", "/apis/v1/kubescape.io/v1/a/b", "/apis/v2/kubescape.io/v1/x"} {
		attrs, ok := Route(httptest.NewRequest(http.MethodPost, p, nil))
		require.True(t, ok, p)
		require.Equal(t, apiauth.Attributes{Group: "kubescape.io", Resource: "unknown", Verb: "create"}, attrs, p)
	}
}

func TestAdapter_RejectsWithoutAuth(t *testing.T) {
	deny := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	}
	a := NewHTTPEndpointAdapter(config.Config{}, deny)
	res := httptest.NewRecorder()
	a.httpMux.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/runtimealerts", nil))
	require.Equal(t, http.StatusUnauthorized, res.Code)
	res = httptest.NewRecorder()
	a.httpMux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, res.Code)
}
