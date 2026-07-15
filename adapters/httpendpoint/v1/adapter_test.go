package httpendpoint

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
)

func newTestAdapter(putObject func(ctx context.Context, id domain.KindName, checksum string, object []byte) error) *Adapter {
	return &Adapter{
		callbacks: domain.Callbacks{
			PutObject: putObject,
		},
		supportedPaths: map[domain.Strategy]map[string]map[string]map[string]bool{
			domain.CopyStrategy: {
				"testgroup": {
					"v1": {
						"widget": true,
					},
				},
			},
		},
	}
}

func TestServeHTTP_BodyWithinLimit(t *testing.T) {
	putObjectCalled := false
	a := newTestAdapter(func(_ context.Context, _ domain.KindName, _ string, _ []byte) error {
		putObjectCalled = true
		return nil
	})

	body := []byte(`{"apiVersion":"testgroup/v1","kind":"Widget","metadata":{"name":"w1"}}`)
	r := httptest.NewRequest(http.MethodPost, "/apis/v1/testgroup/v1/widget", bytes.NewReader(body))
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)

	assert.Equal(t, http.StatusAccepted, w.Code)
	assert.True(t, putObjectCalled)
}

func TestServeHTTP_BodyTooLarge(t *testing.T) {
	putObjectCalled := false
	a := newTestAdapter(func(_ context.Context, _ domain.KindName, _ string, _ []byte) error {
		putObjectCalled = true
		return nil
	})

	body := make([]byte, maxRequestBodyBytes+1)
	r := httptest.NewRequest(http.MethodPost, "/apis/v1/testgroup/v1/widget", bytes.NewReader(body))
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	assert.False(t, putObjectCalled)
}
