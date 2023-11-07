package core

import (
	"context"
	"testing"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
)

func TestSynchronizer_HandleSyncGetObject(t *testing.T) {
	synchronizer := &Synchronizer{
		adapter: adapters.NewMockAdapter(),
		outPool: nil,
	}

	err := synchronizer.handleSyncGetObject(context.TODO(), domain.KindName{}, []byte("baseObject"))
	assert.ErrorContains(t, err, "object not found")
}
