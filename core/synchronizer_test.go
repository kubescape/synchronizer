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
		Kind:      domain.KindFromString("apps/v1/Deployment"),
		Name:      "name",
		Namespace: "namespace",
	}
	kindKnownServers = domain.KindName{
		Kind:      domain.KindFromString("spdx.softwarecomposition.kubescape.io/v1beta1/KnownServers"),
		Name:      "name",
		Namespace: "namespace",
	}
	object = []byte(`{"kind":"kind","metadata":{"name":"name"}}`)
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
	client := NewSynchronizerClient(ctx, clientAdapter, clientConn)
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
	objectV2 := []byte(`{"kind":"kind","metadata":{"name":"name","version":"2"}}`)
	err := clientAdapter.TestCallPutOrPatch(ctx, kindDeployment, object, objectV2)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check object modified
	serverObj, ok := serverAdapter.Resources[kindDeployment.String()]
	assert.True(t, ok)
	assert.Equal(t, objectV2, serverObj)
}
