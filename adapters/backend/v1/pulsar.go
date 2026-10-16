package backend

import (
	"context"
	"fmt"
	"github.com/kubescape/synchronizer/telemetry"
	"go.opentelemetry.io/otel/propagation"
	"os"
	"strings"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
)

// ******************************
// * Pulsar Message Reader  *//
// ******************************

type PulsarMessageReader struct {
	name           string
	reader         pulsar.Reader
	done           chan bool
	messageChannel chan pulsar.Message
	wg             sync.WaitGroup
	workers        int
	handler        *messaging.MessageHandler
}

var _ messaging.MessageReader = (*PulsarMessageReader)(nil)

func NewPulsarMessageReader(cfg config.Config, pulsarClient pulsarconnector.Client) (*PulsarMessageReader, error) {
	workers := 10 // default
	if cfg.Backend.ConsumerWorkers > 0 {
		workers = cfg.Backend.ConsumerWorkers
	}

	msgChannel := make(chan pulsar.Message, workers)

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	readerName := fmt.Sprintf("%s-%s", cfg.Backend.Subscription, hostname)
	topic := pulsarconnector.BuildPersistentTopic(pulsarClient.GetConfig().Tenant, pulsarClient.GetConfig().Namespace, cfg.Backend.ConsumerTopic)
	logger.L().Debug("creating new pulsar reader",
		helpers.String("readerName", readerName),
		helpers.String("topic", topic))

	reader, err := pulsarClient.CreateReader(
		pulsar.ReaderOptions{
			Name:                    readerName,
			Topic:                   topic,
			StartMessageID:          pulsar.LatestMessageID(),
			StartMessageIDInclusive: true,
		})

	if err != nil {
		panic(err)
	}

	return &PulsarMessageReader{
		name:           readerName,
		reader:         reader,
		done:           make(chan bool),
		messageChannel: msgChannel,
		workers:        workers,
		handler:        &messaging.MessageHandler{Name: readerName},
	}, nil
}

func (c *PulsarMessageReader) Start(mainCtx context.Context, adapter adapters.Adapter) {
	go func() {
		logger.L().Info("starting to read messages from pulsar")
		c.readerLoop(mainCtx)
	}()

	go func() {
		for w := 1; w <= c.workers; w++ {
			logger.L().Info("starting to listening on pulsar message channel", helpers.Int("worker", w))
			c.wg.Add(1)
			go c.listenOnMessageChannel(mainCtx, adapter)
		}

		c.wg.Wait()
		c.stop()
	}()
}

func (c *PulsarMessageReader) stop() {
	logger.L().Info("closing pulsar reader")
	c.reader.Close()
	logger.L().Info("closing pulsar message channel")
	close(c.messageChannel)
	c.done <- true
}

func (c *PulsarMessageReader) readerLoop(ctx context.Context) {
	for {
		msg, err := c.reader.Next(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				logger.L().Ctx(ctx).Info("pulsar reader loop exiting due to context cancellation")
				return
			default:
			}
			if strings.Contains(err.Error(), "ConsumerClosed") || strings.Contains(err.Error(), "consumer closed") {
				logger.L().Ctx(ctx).Info("pulsar reader loop exiting due to consumer close")
				return
			}
			logger.L().Ctx(ctx).Fatal("failed to read message from pulsar", helpers.Error(err))
		}

		if messaging.IsSelfProducedMessage(msg.Properties(), msg.Key()) {
			continue
		}

		msgID := utils.PulsarMessageIDtoString(msg.ID())

		select {
		case c.messageChannel <- msg:
			logger.L().Ctx(ctx).Debug("pulsar message enqueued", helpers.String("msgId", msgID))
		case <-c.done:
			logger.L().Ctx(ctx).Fatal("pulsar message will not be processed because channel was closed", helpers.String("msgId", msgID))
		}
	}
}

func (c *PulsarMessageReader) listenOnMessageChannel(ctx context.Context, adapter adapters.Adapter) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.messageChannel:
			msgID := utils.PulsarMessageIDtoString(msg.ID())
			incoming := messaging.IncomingMessage{
				ID:         msgID,
				Properties: msg.Properties(),
				Payload:    msg.Payload(),
			}
			if err := c.handler.Handle(ctx, adapter, incoming); err != nil {
				logger.L().Ctx(ctx).Error("failed to handle message", helpers.Error(err), helpers.String("msgId", msgID))
			} else {
				logger.L().Ctx(ctx).Debug("message processed successfully", helpers.String("msgId", msgID))
			}
		}
	}
}

// ******************************
// * Pulsar Message Producer  *//
// ******************************

type PulsarMessageProducer struct {
	producer pulsar.Producer
}

func NewPulsarMessageProducer(cfg config.Config, pulsarClient pulsarconnector.Client) (*PulsarMessageProducer, error) {
	topic := cfg.Backend.ProducerTopic
	fullTopic := pulsarconnector.BuildPersistentTopic(pulsarClient.GetConfig().Tenant, pulsarClient.GetConfig().Namespace, topic)

	options := pulsar.ProducerOptions{
		DisableBatching:  true,
		EnableChunking:   true,
		CompressionType:  pulsar.ZSTD,
		CompressionLevel: 1,
		Properties: map[string]string{
			"podName": os.Getenv("HOSTNAME"),
		},
		Topic: fullTopic,
	}

	producer, err := pulsarClient.CreateProducer(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for topic '%s': %w", topic, err)
	}

	return &PulsarMessageProducer{producer: producer}, nil
}

func (p *PulsarMessageProducer) ProduceMessage(ctx context.Context, id domain.ClientIdentifier, eventType string, payload []byte, optionalProperties ...map[string]string) error {
	finish := telemetry.Default().Begin(ctx, "mq_produce")

	producerMessage := NewProducerMessage(messaging.SynchronizerServerProducerKey, id.Account, id.Cluster, eventType, payload, optionalProperties...)
	propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(producerMessage.Properties))
	p.producer.SendAsync(ctx, producerMessage, func(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
		finish(err, len(payload))
		logPulsarSyncAsyncErrors(id, msg, err)
	})
	return nil
}

func (p *PulsarMessageProducer) ProduceMessageWithoutIdentifier(ctx context.Context, eventType string, payload []byte) error {
	finish := telemetry.Default().Begin(ctx, "mq_produce")
	producerMessage := NewProducerMessage(messaging.SynchronizerServerProducerKey, "", "", eventType, payload)
	propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(producerMessage.Properties))
	p.producer.SendAsync(ctx, producerMessage, func(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
		finish(err, len(payload))
		logPulsarSyncAsyncErrors(id, msg, err)
	})
	return nil
}

func logPulsarSyncAsyncErrors(msgID pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
	var msgIdStr string
	if msgID != nil {
		msgIdStr = msgID.String()
	}

	var messageProperties map[string]string
	var messagePayloadBytes int
	if message != nil {
		messagePayloadBytes = len(message.Payload)
		messageProperties = message.Properties
	}

	status := "success"
	if err != nil {
		status = "error"
		logger.L().Error("failed to send message to pulsar",
			helpers.Error(err),
			helpers.String("messageID", msgIdStr),
			helpers.Int("payloadBytes", messagePayloadBytes),
			helpers.Interface("messageProperties", messageProperties))
	} else {
		logger.L().Debug("successfully sent message to pulsar", helpers.String("messageID", msgIdStr), helpers.Interface("messageProperties", messageProperties))
	}

	messaging.RecordProducerMessage("pulsar", status, messageProperties, messagePayloadBytes)
}

// NewProducerMessage builds a Pulsar producer message. producerMessageKey sets
// the message Key (used for Pulsar partitioning/ordering); callers within the
// synchronizer server pass messaging.SynchronizerServerProducerKey, while other
// producers of this exported helper (e.g. event-ingester-service) supply their
// own key. Keeping this parameter preserves the public signature that external
// consumers of adapters/backend/v1 depend on.
func NewProducerMessage(producerMessageKey, account, cluster, eventType string, payload []byte, optionalProperties ...map[string]string) *pulsar.ProducerMessage {
	properties := messaging.BuildProducerProperties(account, cluster, eventType, optionalProperties...)

	// BuildProducerProperties always stamps the self-produced source header, which
	// the server's read loop uses to skip its own messages (IsSelfProducedMessage).
	// Only the synchronizer server (identified by its producer key) may claim that
	// source; external producers such as event-ingester-service supply their own
	// key and must NOT be tagged self-produced, or the server would silently drop
	// their backend->cluster messages.
	if producerMessageKey != messaging.SynchronizerServerProducerKey {
		delete(properties, messaging.MsgPropProducerSource)
	}
	return &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: properties,
		Key:        producerMessageKey,
	}
}
