package messaging

import (
	"context"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
)

type MessageProducer interface {
	// ProduceMessage produces a message to a messaging system
	ProduceMessage(ctx context.Context, id domain.ClientIdentifier, eventType string, payload []byte, optionalProperties ...map[string]string) error
}

type MessageReader interface {
	// Start starts the message reader and blocks until the context is done
	Start(mainContext context.Context, adapter adapters.Adapter)
}
