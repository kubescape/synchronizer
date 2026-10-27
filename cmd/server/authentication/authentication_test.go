package authentication

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
)

const validAccount = "123e4567-e89b-12d3-a456-426614174000"

// trackedBody records whether the consumer read it to EOF and closed it.
type trackedBody struct {
	r      io.Reader
	eof    bool
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		b.eof = true
	}
	return n, err
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// setup resets the package-level client/once and installs a fake authentication
// server. It returns the middleware under test and a pointer to the request the
// fake server saw (nil until a request is made).
func setup(t *testing.T, cfg *config.AuthenticationServerConfig, rt roundTripFunc, next http.Handler) (http.Handler, **http.Request) {
	t.Helper()
	once = sync.Once{}
	client = nil
	t.Cleanup(func() {
		once = sync.Once{}
		client = nil
	})

	var seen *http.Request
	handler := AuthenticationServerMiddleware(cfg, next)
	// Consume the sync.Once so the middleware keeps our fake client instead of
	// building a real one, then install it.
	once.Do(func() {})
	client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = r
		return rt(r)
	})}
	return handler, &seen
}

func validRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(core.AccessKeyHeader, "11111111-2222-3333-4444-555555555555")
	r.Header.Set(core.AccountHeader, validAccount)
	r.Header.Set(core.ClusterNameHeader, "cluster-a")
	return r
}

func TestMiddleware_ReleasesAuthResponseBody(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantStatus int
		wantNext   bool
	}{
		{"authorized", http.StatusOK, `{"ok":true}`, http.StatusOK, true},
		{"unauthorized", http.StatusUnauthorized, `{"error":"bad key"}`, http.StatusUnauthorized, false},
		{"server error", http.StatusInternalServerError, "boom", http.StatusServiceUnavailable, false},
		{"empty body", http.StatusOK, "", http.StatusOK, true},
		// Larger than maxDrainBytes: must still be closed, without reading it all.
		{"oversized body", http.StatusOK, strings.Repeat("x", 3*maxDrainBytes), http.StatusOK, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackedBody{r: strings.NewReader(tc.body)}
			nextCalled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nextCalled = true
				// The body must already be released while the downstream handler
				// (the long-lived sync connection) is running.
				if !body.closed {
					t.Error("auth response body still open while next handler runs")
				}
				w.WriteHeader(http.StatusOK)
			})
			h, _ := setup(t, &config.AuthenticationServerConfig{Url: "http://auth.invalid/check"},
				func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: tc.status, Body: body, Header: http.Header{}}, nil
				}, next)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, validRequest())

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if nextCalled != tc.wantNext {
				t.Errorf("next called = %v, want %v", nextCalled, tc.wantNext)
			}
			if !body.closed {
				t.Error("auth response body was never closed")
			}
			if len(tc.body) <= maxDrainBytes && !body.eof {
				t.Error("small auth response body was closed before being drained, which prevents connection reuse")
			}
		})
	}
}

func TestMiddleware_TransportError(t *testing.T) {
	nextCalled := false
	h, _ := setup(t, &config.AuthenticationServerConfig{Url: "http://auth.invalid/check"},
		func(*http.Request) (*http.Response, error) { return nil, errors.New("connection refused") },
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { nextCalled = true }))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, validRequest())

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if nextCalled {
		t.Error("next handler must not run when the auth server is unreachable")
	}
}

func TestMiddleware_RejectsBeforeCallingAuthServer(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*http.Request)
		wantStatus int
	}{
		{"missing access key", func(r *http.Request) { r.Header.Del(core.AccessKeyHeader) }, http.StatusUnauthorized},
		{"missing account", func(r *http.Request) { r.Header.Del(core.AccountHeader) }, http.StatusUnauthorized},
		{"missing cluster", func(r *http.Request) { r.Header.Del(core.ClusterNameHeader) }, http.StatusUnauthorized},
		{"invalid synchronizer version", func(r *http.Request) { r.Header.Set(core.VersionHeader, "invalid") }, http.StatusFailedDependency},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			h, _ := setup(t, &config.AuthenticationServerConfig{Url: "http://auth.invalid/check"},
				func(*http.Request) (*http.Response, error) {
					called = true
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
				},
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("next handler must not run") }))

			r := validRequest()
			tc.mutate(r)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if called {
				t.Error("auth server must not be contacted for a request that is rejected up front")
			}
		})
	}
}

func TestMiddleware_ForwardsMappedHeadersAndSetsClientIdentifier(t *testing.T) {
	cfg := &config.AuthenticationServerConfig{
		Url:                       "http://auth.invalid/check?fixed=1",
		HeaderToQueryParamMapping: map[string]string{core.AccountHeader: "customer"},
		HeaderToHeaderMapping:     map[string]string{core.AccessKeyHeader: "Authorization"},
	}
	var got domain.ClientIdentifier
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = r.Context().Value(domain.ContextKeyClientIdentifier).(domain.ClientIdentifier)
	})
	h, seen := setup(t, cfg, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	}, next)

	r := validRequest()
	r.Header.Set(core.VersionHeader, "v1.2.3")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if *seen == nil {
		t.Fatal("auth server was not contacted")
	}
	q := (*seen).URL.Query()
	if q.Get("customer") != validAccount {
		t.Errorf("query param customer = %q, want %q", q.Get("customer"), validAccount)
	}
	if q.Get("fixed") != "1" {
		t.Errorf("pre-existing query param was dropped: %v", q)
	}
	if auth := (*seen).Header.Get("Authorization"); auth != r.Header.Get(core.AccessKeyHeader) {
		t.Errorf("Authorization header = %q, want the access key", auth)
	}
	if got.Account != validAccount || got.Cluster != "cluster-a" || got.SyncVersion != "v1.2.3" {
		t.Errorf("client identifier = %+v", got)
	}
}

func TestMiddleware_NoAuthServerConfigured(t *testing.T) {
	once = sync.Once{}
	client = nil
	t.Cleanup(func() { once = sync.Once{}; client = nil })

	nextCalled := false
	h := AuthenticationServerMiddleware(&config.AuthenticationServerConfig{}, http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) { nextCalled = true }))
	h.ServeHTTP(httptest.NewRecorder(), validRequest())

	if !nextCalled {
		t.Error("with no authentication server configured, connections must pass through")
	}
}

func TestUnconfiguredCredentialsReason(t *testing.T) {
	const goodKey = "11111111-2222-3333-4444-555555555555"
	tests := []struct {
		name, account, key string
		wantReason         bool
	}{
		{"helm placeholder", "REPLACE_ME_WITH_ARMO_ACCOUNT_UUID", goodKey, true},
		{"bare test account", "test", goodKey, true},
		{"empty account", "", goodKey, true},
		{"account not a uuid", "my-company", goodKey, true},
		{"short access key", validAccount, "short", true},
		{"plausible credentials", validAccount, goodKey, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := unconfiguredCredentialsReason(tc.account, tc.key)
			if (got != "") != tc.wantReason {
				t.Errorf("reason = %q, wantReason = %v", got, tc.wantReason)
			}
			if strings.Contains(got, tc.key) && tc.key != "" {
				t.Error("reason must never contain the raw access key")
			}
		})
	}
}

// stalledBody never produces data: Read blocks until the body is closed.
type stalledBody struct {
	once   sync.Once
	closed chan struct{}
}

func newStalledBody() *stalledBody { return &stalledBody{closed: make(chan struct{})} }

func (b *stalledBody) Read([]byte) (int, error) {
	<-b.closed
	return 0, http.ErrBodyReadAfterClose
}

func (b *stalledBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func shortDrainTimeout(t *testing.T) {
	t.Helper()
	orig := drainTimeout
	drainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { drainTimeout = orig })
}

func TestDiscardBody_GivesUpOnAStalledBody(t *testing.T) {
	shortDrainTimeout(t)
	body := newStalledBody()

	returned := make(chan struct{})
	go func() {
		discardBody(body)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("discardBody is still blocked on a body that stalled")
	}
	select {
	case <-body.closed:
	default:
		t.Error("the stalled body was not closed")
	}
}

// An authentication server can send its status and headers and then stall before
// the end of the body. That must not hold up admitting or rejecting the connection,
// and the connection to the authentication server must be released afterwards.
func TestMiddleware_StalledAuthResponseBodyDoesNotBlockAuthentication(t *testing.T) {
	tests := []struct {
		status   int
		wantNext bool
	}{
		{http.StatusOK, true},
		{http.StatusUnauthorized, false},
	}
	for _, tc := range tests {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			shortDrainTimeout(t)
			released := make(chan struct{}, 1)
			authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "1000") // promise a body...
				w.WriteHeader(tc.status)
				w.(http.Flusher).Flush() // ...send the headers...
				<-r.Context().Done()     // ...then stall until the client goes away
				released <- struct{}{}
			}))
			t.Cleanup(func() {
				authServer.CloseClientConnections()
				authServer.Close()
			})

			// Use the real client: let the middleware build it.
			once = sync.Once{}
			client = nil
			t.Cleanup(func() {
				once = sync.Once{}
				client = nil
			})
			nextCalled := false
			h := AuthenticationServerMiddleware(
				&config.AuthenticationServerConfig{Url: authServer.URL},
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					nextCalled = true
					w.WriteHeader(http.StatusOK)
				}))

			rec := httptest.NewRecorder()
			finished := make(chan struct{})
			go func() {
				h.ServeHTTP(rec, validRequest())
				close(finished)
			}()
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("authentication did not complete while the auth server stalled its response body")
			}

			wantStatus := http.StatusOK
			if !tc.wantNext {
				wantStatus = http.StatusUnauthorized
			}
			if rec.Code != wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, wantStatus)
			}
			if nextCalled != tc.wantNext {
				t.Errorf("next called = %v, want %v", nextCalled, tc.wantNext)
			}

			select {
			case <-released:
			case <-time.After(3 * time.Second):
				t.Error("the connection to the auth server was not released")
			}
		})
	}
}
