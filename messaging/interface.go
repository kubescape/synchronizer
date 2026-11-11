package messaging

import (
	"context"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
)

type MessageProducer interface {
	// ProduceMessage produces a message to a messaging system
	ProduceMessage(ctx context.Context, id domain.ClientIdentifier, eventType string, payload []byte, optionalProperties ...map[string]string) error
	ProduceMessageWithoutIdentifier(ctx context.Context, eventType string, payload []byte) error
}

type MessageReader interface {
	// Start starts the message reader and blocks until the context is done
	Start(mainContext context.Context, adapter adapters.Adapter)
}

// ReaderWorkers keeps broker receive order when the adapter needs ordered replay.
// Otherwise the configured worker count retains its existing meaning.
func ReaderWorkers(adapter adapters.Adapter, configured int) int {
	if ordered, ok := adapter.(interface{ OrderedBackendMessages() bool }); ok && ordered.OrderedBackendMessages() {
		return 1
	}
	return configured
}
