package httpendpoint

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
)

type unreadBody struct{ t *testing.T }

func (b unreadBody) Read([]byte) (int, error) {
	b.t.Error("rejected request body was read")
	return 0, io.EOF
}
func (b unreadBody) Close() error { return nil }

func testAdapter(available bool) *Adapter {
	a := NewHTTPEndpointAdapter(config.Config{})
	a.callbacks.BackendAvailable = func() bool { return available }
	a.supportedPaths = map[domain.Strategy]map[string]map[string]map[string]bool{
		domain.CopyStrategy: {"kubescape.io": {"v1": {"networkstreams": true, "runtimealerts": true}}},
	}
	return a
}

func TestBackpressureBeforeBodyRead(t *testing.T) {
	for _, unavailable := range []bool{true, false} {
		a := testAdapter(!unavailable)
		if !unavailable {
			for range 8 {
				if !a.admit("networkstreams") {
					t.Fatal("early rejection")
				}
			}
		}
		r := httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/networkstreams", nil)
		r.Body = unreadBody{t}
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "60" {
			t.Fatalf("response: %d %v", w.Code, w.Header())
		}
	}
}

func TestAdmissionReservesAlertCapacity(t *testing.T) {
	a := testAdapter(true)
	for range 8 {
		if !a.admit("networkstreams") {
			t.Fatal("early rejection")
		}
	}
	if a.admit("networkstreams") {
		t.Fatal("telemetry exceeded limit")
	}
	for range 2 {
		if !a.admit("runtimealerts") {
			t.Fatal("alert reservation unavailable")
		}
	}
	if a.admit("runtimealerts") {
		t.Fatal("total limit exceeded")
	}
	a.release("networkstreams")
	if !a.admit("networkstreams") {
		t.Fatal("released slot leaked")
	}
}

func TestAdmissionConcurrentBound(t *testing.T) {
	a := testAdapter(true)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() { a.admit("networkstreams") })
	}
	wg.Wait()
	if a.admitted != 8 || a.telemetry != 8 {
		t.Fatalf("admitted %d telemetry %d", a.admitted, a.telemetry)
	}
}

func TestAdmissionReleasedAfterInvalidBody(t *testing.T) {
	a := testAdapter(true)
	for range 20 {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/networkstreams", strings.NewReader("invalid")))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status %d", w.Code)
		}
	}
	if a.admitted != 0 {
		t.Fatal("slot leaked")
	}
}

func TestAcceptedRequest(t *testing.T) {
	a := testAdapter(true)
	called := false
	a.callbacks.PutObject = func(context.Context, domain.KindName, string, []byte) error { called = true; return nil }
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/networkstreams", strings.NewReader(`{"apiVersion":"kubescape.io/v1","kind":"NetworkStreams","metadata":{"name":"test"}}`)))
	if w.Code != http.StatusAccepted || !called || a.admitted != 0 {
		t.Fatalf("status %d callback %v slots %d", w.Code, called, a.admitted)
	}
}

func TestInvalidRoutesBeforeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		status             int
	}{
		{"invalid path", http.MethodPost, "/invalid", http.StatusBadRequest},
		{"invalid prefix", http.MethodPost, "/wrong/v1/kubescape.io/v1/networkstreams", http.StatusBadRequest},
		{"unsupported method", http.MethodDelete, "/apis/v1/kubescape.io/v1/networkstreams", http.StatusMethodNotAllowed},
		{"unsupported group", http.MethodPost, "/apis/v1/other.io/v1/networkstreams", http.StatusNotFound},
		{"unsupported version", http.MethodPost, "/apis/v1/kubescape.io/v2/networkstreams", http.StatusNotFound},
		{"unsupported resource", http.MethodPost, "/apis/v1/kubescape.io/v1/other", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testAdapter(false)
			a.callbacks.BackendAvailable = func() bool { t.Error("invalid route consulted admission"); return false }
			r := httptest.NewRequest(tc.method, tc.path, nil)
			r.Body = unreadBody{t}
			w := httptest.NewRecorder()
			a.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d", w.Code, tc.status)
			}
			if a.admitted != 0 || a.telemetry != 0 {
				t.Fatal("invalid route reserved capacity")
			}
		})
	}
}

func TestOversizedBodyReleasesAdmission(t *testing.T) {
	a := testAdapter(true)
	a.callbacks.PutObject = func(context.Context, domain.KindName, string, []byte) error {
		t.Error("oversized body forwarded")
		return nil
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/networkstreams", strings.NewReader(strings.Repeat("x", maxRequestBodyBytes+1))))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", w.Code)
	}
	if a.admitted != 0 || a.telemetry != 0 {
		t.Fatal("oversized body leaked admission")
	}
}

func TestAlertRequestUsesReservedCapacity(t *testing.T) {
	a := testAdapter(true)
	for range 8 {
		if !a.admit("networkstreams") {
			t.Fatal("early rejection")
		}
	}
	called := false
	a.callbacks.PutObject = func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
		called = true
		if id.Kind.Resource != "runtimealerts" {
			t.Errorf("resource = %s", id.Kind.Resource)
		}
		return nil
	}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/runtimealerts", strings.NewReader(`{"apiVersion":"kubescape.io/v1","kind":"RuntimeAlerts","metadata":{"name":"test"}}`)))
	if w.Code != http.StatusAccepted || !called {
		t.Fatalf("status = %d, called = %v", w.Code, called)
	}
	if a.admitted != 8 || a.telemetry != 8 {
		t.Fatal("alert leaked admission")
	}
	for range 2 {
		if !a.admit("runtimealerts") {
			t.Fatal("alert reservation unavailable")
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/runtimealerts", nil)
	r.Body = unreadBody{t}
	w = httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "60" {
		t.Fatalf("response = %d %v", w.Code, w.Header())
	}
}

func TestBackpressureWithoutAvailabilityCallback(t *testing.T) {
	a := testAdapter(true)
	a.callbacks.BackendAvailable = nil
	r := httptest.NewRequest(http.MethodPost, "/apis/v1/kubescape.io/v1/networkstreams", nil)
	r.Body = unreadBody{t}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "60" {
		t.Fatalf("response: %d %v", w.Code, w.Header())
	}
}

func TestBackpressureFlushesWithoutDrainingBody(t *testing.T) {
	a := testAdapter(false)
	srv := httptest.NewServer(a)
	defer srv.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Send only headers: an early rejection must arrive without waiting for body bytes.
	if _, err := io.WriteString(conn, "POST /apis/v1/kubescape.io/v1/networkstreams HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("rejection waited for unread body: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests || !response.Close || response.Header.Get("Retry-After") != "60" {
		t.Fatalf("response: %d close=%v headers=%v", response.StatusCode, response.Close, response.Header)
	}
}
