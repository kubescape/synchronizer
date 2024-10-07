package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
