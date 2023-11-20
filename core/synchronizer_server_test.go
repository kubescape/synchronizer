package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSynchronizer_ObjectAddedOnServer(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// add object
	err := serverAdapter.TestCallVerifyObject(ctx, kindKnownServers, object)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check object added
	clientObj, ok := clientAdapter.Resources[kindKnownServers.String()]
	assert.True(t, ok)
	assert.Equal(t, object, clientObj)
}

func TestSynchronizer_ObjectDeletedOnServer(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindKnownServers.String()] = object
	serverAdapter.Resources[kindKnownServers.String()] = object
	// delete object
	err := serverAdapter.TestCallDeleteObject(ctx, kindKnownServers)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check object deleted
	_, ok := clientAdapter.Resources[kindKnownServers.String()]
	assert.False(t, ok)
}

func TestSynchronizer_ObjectModifiedOnServer(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindKnownServers.String()] = object
	serverAdapter.Resources[kindKnownServers.String()] = object
	// modify object
	objectV2 := []byte(`{"kind":"kind","metadata":{"name":"name","version":"2"}}`)
	err := serverAdapter.TestCallPutOrPatch(ctx, kindKnownServers, nil, objectV2)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	// check object modified
	clientObj, ok := clientAdapter.Resources[kindKnownServers.String()]
	assert.True(t, ok)
	assert.Equal(t, objectV2, clientObj)
}
