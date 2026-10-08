package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
)

type lifecycleAdapter struct {
	*adapters.MockAdapter
	started chan struct{}
}

func (a *lifecycleAdapter) Start(ctx context.Context) error {
	err := a.MockAdapter.Start(ctx)
	close(a.started)
	return err
}

func TestWebSocketHandlerKeepsWriterAlive(t *testing.T) {
	a := &lifecycleAdapter{MockAdapter: adapters.NewMockAdapter(false), started: make(chan struct{})}
	handler := synchronizationHandler(a)
	returned := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
		handler.ServeHTTP(w, r.WithContext(ctx))
		close(returned)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, _, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case <-a.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-returned:
		t.Fatal("upgrade handler returned before connection closed")
	default:
	}
	callbacks, _ := a.Callbacks(ctx)
	if !callbacks.BackendAvailable() {
		t.Fatal("upgraded connection writer unavailable")
	}
	sendCtx := context.WithValue(ctx, domain.ContextKeyClientIdentifier, domain.ClientIdentifier{})
	kind := domain.KindName{Kind: domain.KindFromString(ctx, "apps/v1/deployments"), Name: "test"}
	if err := callbacks.PutObject(sendCtx, kind, "", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := wsutil.ReadServerBinary(conn); err != nil {
		t.Fatalf("server writer failed: %v", err)
	}
	conn.Close()
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("handler did not exit on closed connection")
	}
}
