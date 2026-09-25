package backend

import (
	"context"
	"sync"
	"testing"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
)

// Exercise reads racing the first registration, when a zero-value SafeMap
// would otherwise initialize its backing map concurrently with an unlocked read.
func TestAdapterConcurrentFirstRegistration(t *testing.T) {
	adapter := NewBackendAdapter(context.Background(), nil, config.Backend{})
	id := domain.ClientIdentifier{Account: "account", Cluster: "cluster"}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 1000 {
			adapter.IsRelated(context.Background(), id)
		}
	})
	wg.Go(func() {
		for range 1000 {
			adapter.callbacksMap.Set(id.String(), domain.Callbacks{})
			adapter.clientsMap.Set(id.String(), &Client{})
		}
	})
	wg.Wait()
}
