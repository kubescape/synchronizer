package core

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
)

var (
	kindDeployment = domain.KindName{
		Kind:      domain.KindFromString(context.TODO(), "apps/v1/Deployment"),
		Name:      "name",
		Namespace: "namespace",
	}
	kindKnownServers = domain.KindName{
		Kind:      domain.KindFromString(context.TODO(), "spdx.softwarecomposition.kubescape.io/v1beta1/KnownServers"),
		Name:      "name",
		Namespace: "namespace",
	}
	object         = []byte(`{"kind":"kind","metadata":{"name":"name","resourceVersion":"1"}}`)
	objectClientV2 = []byte(`{"kind":"kind","metadata":{"name":"client","resourceVersion":"2"}}`)
	objectServerV2 = []byte(`{"kind":"kind","metadata":{"name":"server","resourceVersion":"2"}}`)
)

func initTest(t *testing.T) (context.Context, *adapters.MockAdapter, *adapters.MockAdapter) {
	ctx := context.WithValue(context.TODO(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: "11111111-2222-3333-4444-555555555555",
		Cluster: "cluster",
	})
	clientAdapter := adapters.NewMockAdapter(true)
	serverAdapter := adapters.NewMockAdapter(false)
	clientConn, serverConn := net.Pipe()
	newConn := func() (net.Conn, error) {
		return clientConn, nil
	}
	client, err := NewSynchronizerClient(ctx, []adapters.Adapter{clientAdapter}, clientConn, newConn)
	assert.NoError(t, err)
	server, err := NewSynchronizerServer(ctx, []adapters.Adapter{serverAdapter}, serverConn)
	assert.NoError(t, err)
	go func() {
		_ = client.Start(ctx)
	}()
	go func() {
		_ = server.Start(ctx)
	}()
	return ctx, clientAdapter, serverAdapter
}

func TestSynchronizer_ObjectModifiedOnBothSides(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.StoreResource(kindKnownServers.String(), object)
	serverAdapter.StoreResource(kindKnownServers.String(), object)
	// manually modify object on server (PutObject message will be sent later)
	serverAdapter.StoreResource(kindKnownServers.String(), objectServerV2)
	// we create a race condition here
	// object is modified on client, but we don't know about server modification
	err := clientAdapter.TestCallPutOrPatch(ctx, kindKnownServers, object, objectClientV2)
	assert.NoError(t, err)
	// server message arrives just now on client
	err = clientAdapter.PutObject(ctx, kindKnownServers, "", objectServerV2)
	assert.NoError(t, err)
	// check both sides have the one from the server
	assertResourceEventually(t, clientAdapter, kindKnownServers.String(), objectServerV2)
	assertResourceEventually(t, serverAdapter, kindKnownServers.String(), objectServerV2)
}

func assertResourceEventually(t *testing.T, adapter *adapters.MockAdapter, key string, expected []byte) {
	t.Helper()
	assert.Eventually(t, func() bool {
		actual, ok := adapter.LoadResource(key)
		return ok && bytes.Equal(expected, actual)
	}, 5*time.Second, 10*time.Millisecond)
}
