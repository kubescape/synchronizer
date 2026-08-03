package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/messaging"
)

func init() {
	messaging.RegisterFromConfigFactory(newFromConfig)
}

func newFromConfig(ctx context.Context, cfg config.Config) (*messaging.Components, error) {
	if mq := cfg.Backend.MessageQueue; mq != nil && mq.Type != "" {
		switch mq.Type {
		case "kafka":
			return newKafkaFromConfig(ctx, cfg)
		case "pulsar":
			return newPulsarFromConfig(ctx, cfg)
		default:
			return nil, fmt.Errorf("unknown message queue type %q", mq.Type)
		}
	}
	return newPulsarFromConfig(ctx, cfg)
}

// the pulsar client does its own connection retries internally, so it takes no context here
func newPulsarFromConfig(_ context.Context, cfg config.Config) (*messaging.Components, error) {
	if cfg.Backend.PulsarConfig == nil {
		return nil, nil
	}

	logger.L().Info("initializing pulsar client")
	pulsarClient, err := pulsarconnector.NewClient(
		pulsarconnector.WithConfig(cfg.Backend.PulsarConfig),
		pulsarconnector.WithRetryAttempts(20),
		pulsarconnector.WithRetryMaxDelay(3*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pulsar client: %w", err)
	}

	producer, err := NewPulsarMessageProducer(cfg, pulsarClient)
	if err != nil {
		pulsarClient.Close()
		return nil, fmt.Errorf("failed to create pulsar producer: %w", err)
	}

	reader, err := NewPulsarMessageReader(cfg, pulsarClient)
	if err != nil {
		pulsarClient.Close()
		return nil, fmt.Errorf("failed to create pulsar reader: %w", err)
	}

	logger.L().Info("pulsar message queue initialized",
		helpers.String("config", fmt.Sprintf("%+v", cfg.Backend.PulsarConfig)))

	return &messaging.Components{
		Producer: producer,
		Reader:   reader,
		Close:    pulsarClient.Close,
	}, nil
}
