package core

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
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
	err := logger.L().SetLevel(helpers.DebugLevel.String())
	assert.NoError(t, err)
	clientAdapter := adapters.NewMockAdapter(true)
	serverAdapter := adapters.NewMockAdapter(false)
	clientConn, serverConn := net.Pipe()
	newConn := func() (net.Conn, error) {
		return clientConn, nil
	}
	client := NewSynchronizerClient(ctx, clientAdapter, clientConn, newConn)
	server := NewSynchronizerServer(ctx, serverAdapter, serverConn)
	go func() {
		_ = client.Start(ctx)
	}()
	go func() {
		_ = server.Start(ctx)
	}()
	return ctx, clientAdapter, serverAdapter
}

func TestSynchronizer_ObjectAdded(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// add object
	err := clientAdapter.TestCallVerifyObject(ctx, kindDeployment, object)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check object added
	serverObj, ok := serverAdapter.Resources[kindDeployment.String()]
	assert.True(t, ok)
	assert.Equal(t, object, serverObj)
}

func TestSynchronizer_ObjectDeleted(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindDeployment.String()] = object
	serverAdapter.Resources[kindDeployment.String()] = object
	// delete object
	err := clientAdapter.TestCallDeleteObject(ctx, kindDeployment)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check object deleted
	_, ok := serverAdapter.Resources[kindDeployment.String()]
	assert.False(t, ok)
}

func TestSynchronizer_ObjectModified(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindDeployment.String()] = object
	serverAdapter.Resources[kindDeployment.String()] = object
	// modify object
	err := clientAdapter.TestCallPutOrPatch(ctx, kindDeployment, object, objectClientV2)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check object modified
	serverObj, ok := serverAdapter.Resources[kindDeployment.String()]
	assert.True(t, ok)
	assert.Equal(t, objectClientV2, serverObj)
}

func TestSynchronizer_ObjectModifiedOnBothSides(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindKnownServers.String()] = object
	serverAdapter.Resources[kindKnownServers.String()] = object
	// manually modify object on server (PutObject message will be sent later)
	serverAdapter.Resources[kindKnownServers.String()] = objectServerV2
	// we create a race condition here
	// object is modified on client, but we don't know about server modification
	err := clientAdapter.TestCallPutOrPatch(ctx, kindKnownServers, object, objectClientV2)
	assert.NoError(t, err)
	// server message arrives just now on client
	err = clientAdapter.PutObject(ctx, kindKnownServers, objectServerV2)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check both sides have the one from the server
	clientObj, ok := clientAdapter.Resources[kindKnownServers.String()]
	assert.True(t, ok)
	assert.Equal(t, objectServerV2, clientObj)
	serverObj, ok := clientAdapter.Resources[kindKnownServers.String()]
	assert.True(t, ok)
	assert.Equal(t, objectServerV2, serverObj)
}
