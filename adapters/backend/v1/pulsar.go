package backend

import (
	"context"
	"fmt"
	"github.com/kubescape/synchronizer/telemetry"
	"go.opentelemetry.io/otel/propagation"
	"os"
	"sync"
	"time"

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

const (
	// readerRecoveryMaxAttempts is the number of consecutive attempts to
	// replace a lost reader before the process exits.
	readerRecoveryMaxAttempts = 5
	// readerRecoveryInitialBackoff is the delay before the first attempt; it
	// doubles on every following attempt up to readerRecoveryMaxBackoff.
	readerRecoveryInitialBackoff = time.Second
	readerRecoveryMaxBackoff     = 30 * time.Second
	// readerRecoveryHealthyAfter is how long a replacement reader has to
	// survive before an idle topic stops counting against the attempts above.
	readerRecoveryHealthyAfter = time.Minute
)

// readerRecoveryPolicy controls how readerLoop reacts to losing its reader.
type readerRecoveryPolicy struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	healthyAfter   time.Duration
}

func defaultReaderRecoveryPolicy() readerRecoveryPolicy {
	return readerRecoveryPolicy{
		maxAttempts:    readerRecoveryMaxAttempts,
		initialBackoff: readerRecoveryInitialBackoff,
		maxBackoff:     readerRecoveryMaxBackoff,
		healthyAfter:   readerRecoveryHealthyAfter,
	}
}

// backoff returns the delay before the given recovery attempt (1-based).
func (p readerRecoveryPolicy) backoff(attempt int) time.Duration {
	delay := p.initialBackoff
	for i := 1; i < attempt && delay < p.maxBackoff; i++ {
		delay *= 2
	}
	return min(delay, p.maxBackoff)
}

type PulsarMessageReader struct {
	name string
	// readerMu guards reader and stopped: the reader loop replaces the reader
	// while stop() closes it.
	readerMu sync.Mutex
	reader   pulsar.Reader
	stopped  bool
	// newReader creates a replacement for a lost reader.
	newReader func() (pulsar.Reader, error)
	recovery  readerRecoveryPolicy
	// fatal reports an unrecoverable reader loss. It exits the process so
	// that the pod is restarted instead of staying up without consuming.
	fatal          func(msg string, err error)
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

	newReader := func() (pulsar.Reader, error) {
		return pulsarClient.CreateReader(
			pulsar.ReaderOptions{
				Name:                    readerName,
				Topic:                   topic,
				StartMessageID:          pulsar.LatestMessageID(),
				StartMessageIDInclusive: true,
			})
	}

	reader, err := newReader()
	if err != nil {
		panic(err)
	}

	return &PulsarMessageReader{
		name:      readerName,
		reader:    reader,
		newReader: newReader,
		recovery:  defaultReaderRecoveryPolicy(),
		fatal: func(msg string, err error) {
			logger.L().Fatal(msg, helpers.Error(err), helpers.String("readerName", readerName))
		},
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
		for w := 1; w <= messaging.ReaderWorkers(adapter, c.workers); w++ {
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
	c.readerMu.Lock()
	c.stopped = true
	c.reader.Close()
	c.readerMu.Unlock()
	logger.L().Info("closing pulsar message channel")
	close(c.messageChannel)
	c.done <- true
}

// shutdown marks the reader as stopped before its client is closed. Closing
// the client closes this reader too, which the loop has to take for the
// shutdown it is, not for a lost reader to recreate on the closed client.
func (c *PulsarMessageReader) shutdown() {
	c.readerMu.Lock()
	defer c.readerMu.Unlock()
	c.stopped = true
}

func (c *PulsarMessageReader) isStopped() bool {
	c.readerMu.Lock()
	defer c.readerMu.Unlock()
	return c.stopped
}

func (c *PulsarMessageReader) currentReader() pulsar.Reader {
	c.readerMu.Lock()
	defer c.readerMu.Unlock()
	return c.reader
}

// replaceReader swaps in a newly created reader. It reports false, and closes
// the new reader, when the message reader was stopped in the meantime.
func (c *PulsarMessageReader) replaceReader(reader pulsar.Reader) bool {
	c.readerMu.Lock()
	defer c.readerMu.Unlock()
	if c.stopped {
		reader.Close()
		return false
	}
	c.reader = reader
	return true
}

// recoverReader replaces a reader that stopped delivering messages. Every
// attempt waits for an exponential backoff first and counts against
// recovery.maxAttempts; attempts carries the count across calls so that a
// reader which is lost again right after being replaced cannot loop forever.
// It reports false when the loop has to end: the context was cancelled, the
// message reader was stopped, or the attempts were exhausted (after calling
// fatal).
func (c *PulsarMessageReader) recoverReader(ctx context.Context, cause error, attempts *int) bool {
	// release the lost reader; closing an already closed reader is a no-op
	c.currentReader().Close()

	for {
		// a shutdown that started while the reader was being created must end
		// the recovery quietly instead of counting as a failed attempt
		if ctx.Err() != nil {
			logger.L().Ctx(ctx).Info("pulsar reader recovery aborted due to context cancellation")
			return false
		}
		if c.isStopped() {
			logger.L().Ctx(ctx).Info("pulsar reader recovery aborted because the reader was stopped")
			return false
		}
		*attempts++
		if *attempts > c.recovery.maxAttempts {
			c.fatal("pulsar reader lost and could not be recovered, exiting", cause)
			return false
		}

		delay := c.recovery.backoff(*attempts)
		logger.L().Ctx(ctx).Error("pulsar reader lost, recreating it",
			helpers.Error(cause),
			helpers.String("readerName", c.name),
			helpers.Int("attempt", *attempts),
			helpers.Int("maxAttempts", c.recovery.maxAttempts),
			helpers.String("backoff", delay.String()))

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			logger.L().Ctx(ctx).Info("pulsar reader recovery aborted due to context cancellation")
			return false
		case <-timer.C:
		}

		reader, err := c.newReader()
		if err != nil {
			cause = fmt.Errorf("failed to recreate pulsar reader: %w", err)
			continue
		}
		if !c.replaceReader(reader) {
			logger.L().Ctx(ctx).Info("pulsar reader recovery aborted because the reader was stopped")
			return false
		}
		logger.L().Ctx(ctx).Info("pulsar reader recreated", helpers.String("readerName", c.name))
		return true
	}
}

func (c *PulsarMessageReader) readerLoop(ctx context.Context) {
	// consecutive recovery attempts since the reader was last known healthy
	recoveryAttempts := 0
	var lastRecovery time.Time

	for {
		msg, err := c.currentReader().Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.L().Ctx(ctx).Info("pulsar reader loop exiting due to context cancellation")
				return
			}
			if c.isStopped() {
				logger.L().Ctx(ctx).Info("pulsar reader loop exiting because the reader was stopped")
				return
			}
			// Any other error, a closed consumer included, means that this
			// reader will not deliver messages anymore. Returning here would
			// leave the process running without consuming, so replace the
			// reader, and exit the process if that keeps failing.
			if !lastRecovery.IsZero() && time.Since(lastRecovery) >= c.recovery.healthyAfter {
				recoveryAttempts = 0
			}
			if !c.recoverReader(ctx, err, &recoveryAttempts) {
				return
			}
			lastRecovery = time.Now()
			continue
		}
		recoveryAttempts = 0

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
