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
	// check object added
	assertResourceEventually(t, clientAdapter, kindKnownServers.String(), object)
}

func TestSynchronizer_ObjectDeletedOnServer(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.StoreResource(kindKnownServers.String(), object)
	serverAdapter.StoreResource(kindKnownServers.String(), object)
	// delete object
	err := serverAdapter.TestCallDeleteObject(ctx, kindKnownServers)
	assert.NoError(t, err)
	// check object deleted
	assert.Eventually(t, func() bool {
		_, ok := clientAdapter.LoadResource(kindKnownServers.String())
		return !ok
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSynchronizer_ObjectModifiedOnServer(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.StoreResource(kindKnownServers.String(), object)
	serverAdapter.StoreResource(kindKnownServers.String(), object)
	// modify object
	err := serverAdapter.TestCallPutOrPatch(ctx, kindKnownServers, nil, objectServerV2)
	assert.NoError(t, err)
	// check object modified
	assertResourceEventually(t, clientAdapter, kindKnownServers.String(), objectServerV2)
}
