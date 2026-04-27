package authentication

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/stretchr/testify/assert"
)

// resetMiddlewareState clears package-level state between tests so each test
// case re-initializes via sync.Once.
func resetMiddlewareState() {
	once = sync.Once{}
	client = nil
	authCache = sync.Map{}
	authCacheTTL = 0
}

func newRequestWithHeaders(accessKey, account, cluster string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(core.AccessKeyHeader, accessKey)
	r.Header.Set(core.AccountHeader, account)
	r.Header.Set(core.ClusterNameHeader, cluster)
	return r
}

// TestAuthenticationCacheHit verifies that a second connection from the same
// (accessKey, account) within the TTL window does not trigger an upstream
// auth call.
func TestAuthenticationCacheHit(t *testing.T) {
	resetMiddlewareState()

	var upstreamCalls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := &config.AuthenticationServerConfig{
		Url:             upstream.URL,
		CacheTTLSeconds: 60,
	}

	var nextCalls int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nextCalls, 1)
		w.WriteHeader(http.StatusOK)
	})
	handler := AuthenticationServerMiddleware(cfg, next)

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, newRequestWithHeaders("ak-1", "acc-1", "cluster-1"))
		assert.Equal(t, http.StatusOK, rec.Code, "iteration %d", i)
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamCalls),
		"only the first request should hit the upstream auth server")
	assert.Equal(t, int32(5), atomic.LoadInt32(&nextCalls),
		"all requests should reach the next handler")
}

// TestAuthenticationFailureNotCached verifies that a non-200 response from the
// upstream is not stored in the cache: a subsequent request with the same
// credentials still calls the upstream.
func TestAuthenticationFailureNotCached(t *testing.T) {
	resetMiddlewareState()

	var upstreamCalls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer upstream.Close()

	cfg := &config.AuthenticationServerConfig{
		Url:             upstream.URL,
		CacheTTLSeconds: 60,
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("next handler should not be called when auth fails")
	})
	handler := AuthenticationServerMiddleware(cfg, next)

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, newRequestWithHeaders("ak-2", "acc-2", "cluster-2"))
		assert.Equal(t, http.StatusUnauthorized, rec.Code, "iteration %d", i)
	}

	assert.Equal(t, int32(3), atomic.LoadInt32(&upstreamCalls),
		"every failed auth must hit the upstream; the cache must not be poisoned")

	// after a failure, a recovered upstream should still be reachable
	// (entry was never stored, so no expiry to wait for).
	cacheKey := authCacheKey("ak-2", "acc-2")
	_, present := authCache.Load(cacheKey)
	assert.False(t, present, "failed auth must not create a cache entry")
}

// TestAuthenticationCacheExpiry verifies that an expired entry forces a new
// upstream call.
func TestAuthenticationCacheExpiry(t *testing.T) {
	resetMiddlewareState()

	var upstreamCalls int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := &config.AuthenticationServerConfig{
		Url:             upstream.URL,
		CacheTTLSeconds: 60,
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := AuthenticationServerMiddleware(cfg, next)

	// first call populates the cache
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequestWithHeaders("ak-3", "acc-3", "cluster-3"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, int32(1), atomic.LoadInt32(&upstreamCalls))

	// force the cached entry to be in the past
	cacheKey := authCacheKey("ak-3", "acc-3")
	authCache.Store(cacheKey, time.Now().Add(-time.Second))

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequestWithHeaders("ak-3", "acc-3", "cluster-3"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, int32(2), atomic.LoadInt32(&upstreamCalls),
		"expired entry should force a fresh upstream call")
}

// TestAuthenticationCacheDefaultTTL verifies the 600s default is applied when
// CacheTTLSeconds is zero.
func TestAuthenticationCacheDefaultTTL(t *testing.T) {
	resetMiddlewareState()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	cfg := &config.AuthenticationServerConfig{
		Url: upstream.URL,
		// CacheTTLSeconds intentionally zero to exercise the default
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := AuthenticationServerMiddleware(cfg, next)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequestWithHeaders("ak-4", "acc-4", "cluster-4"))
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, time.Duration(defaultCacheTTLSeconds)*time.Second, authCacheTTL,
		"default TTL should be applied when config value is zero")
}
