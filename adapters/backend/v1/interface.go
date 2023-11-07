package backend

import (
	"context"

	"github.com/kubescape/synchronizer/domain"
)

type MessageProducer interface {
	// ProduceMessage produces a message to a messaging system
	ProduceMessage(ctx context.Context, id domain.ClientIdentifier, eventType string, payload []byte) error
}

type MessageConsumer interface {
	// Start starts the message consumer and blocks until the context is done
	Start(mainContext context.Context, adapter *Adapter)
}
