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
	// check object added
	assertResourceEventually(t, serverAdapter, kindDeployment.String(), object)
}

func TestSynchronizer_ObjectDeleted(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.StoreResource(kindDeployment.String(), object)
	serverAdapter.StoreResource(kindDeployment.String(), object)
	// delete object
	err := clientAdapter.TestCallDeleteObject(ctx, kindDeployment)
	assert.NoError(t, err)
	// check object deleted
	assert.Eventually(t, func() bool {
		_, ok := serverAdapter.LoadResource(kindDeployment.String())
		return !ok
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSynchronizer_ObjectModified(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.StoreResource(kindDeployment.String(), object)
	serverAdapter.StoreResource(kindDeployment.String(), object)
	// modify object
	err := clientAdapter.TestCallPutOrPatch(ctx, kindDeployment, object, objectClientV2)
	assert.NoError(t, err)
	// check object modified
	assertResourceEventually(t, serverAdapter, kindDeployment.String(), objectClientV2)
}
