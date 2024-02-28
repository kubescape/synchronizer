package backend

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

const (
	// this will be used to filter out messages by the synchronizer server consumer (so it doesn't process its own messages)
	SynchronizerServerProducerKey = "SynchronizerServerProducer"
)

// ******************************
// * Pulsar Message Reader  *//
// ******************************

type PulsarMessageReader struct {
	name           string
	reader         pulsar.Reader
	messageChannel chan pulsar.Message
	wg             sync.WaitGroup
	workers        int
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
		messageChannel: msgChannel,
		workers:        workers,
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
}

func (c *PulsarMessageReader) readerLoop(ctx context.Context) {
	for {
		msg, err := c.reader.Next(ctx)
		if err != nil {
			panic(err)
		}

		// skip messages sent by the synchronizer server
		if msg.Key() == SynchronizerServerProducerKey {
			continue
		}

		msgID := utils.PulsarMessageIDtoString(msg.ID())

		select {
		case c.messageChannel <- msg:
			logger.L().Ctx(ctx).Debug("pulsar message enqueued", helpers.String("msgId", msgID))
		default:
			logger.L().Ctx(ctx).Warning("pulsar message will not be processed because channel was closed", helpers.String("msgId", msgID))
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
			if err := c.handleSingleSynchronizerMessage(ctx, adapter, msg); err != nil {
				logger.L().Ctx(ctx).Error("failed to handle message", helpers.Error(err), helpers.String("msgId", msgID))
			} else {
				logger.L().Ctx(ctx).Debug("message processed successfully", helpers.String("msgId", msgID))
			}
		}
	}
}

func (c *PulsarMessageReader) handleSingleSynchronizerMessage(ctx context.Context, adapter adapters.Adapter, msg pulsar.Message) error {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	msgProperties := msg.Properties()
	clientIdentifier := domain.ClientIdentifier{
		Account: msgProperties[messaging.MsgPropAccount],
		Cluster: msgProperties[messaging.MsgPropCluster],
	}

	if !adapter.IsRelated(ctx, clientIdentifier) {
		logger.L().Debug("skipping message from pulsar. client is not related to this instance",
			helpers.String("msgId", msgID),
			helpers.Interface("clientIdentifier", clientIdentifier),
			helpers.String("readerName", c.name),
		)
		return nil
	}

	logger.L().Debug("received message from pulsar",
		helpers.String("account", msgProperties[messaging.MsgPropAccount]),
		helpers.String("cluster", msgProperties[messaging.MsgPropCluster]),
		helpers.Interface("event", msgProperties[messaging.MsgPropEvent]),
		helpers.String("msgId", msgID))

	// store in context
	ctx = utils.ContextFromIdentifiers(ctx, clientIdentifier)
	// get callbacks
	callbacks, err := adapter.Callbacks(ctx)
	if err != nil {
		return fmt.Errorf("failed to get callbacks: %w", err)
	}
	// handle message
	switch msgProperties[messaging.MsgPropEvent] {
	case messaging.MsgPropEventValueReconciliationRequestMessage:
		var data messaging.ReconciliationRequestMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}

		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})

		// unwrap the reconciliation request and send batches of NewChecksum for each kind
		event := domain.EventNewChecksum

		for kindStr, objects := range data.KindToObjects {
			kind := domain.KindFromString(ctx, kindStr)
			if kind == nil {
				err = multierr.Append(err, fmt.Errorf("unknown kind %s in batch", kindStr))
				continue
			}

			items := domain.BatchItems{}
			items.NewChecksum = []domain.NewChecksum{}

			for _, object := range objects {
				items.NewChecksum = append(items.NewChecksum, domain.NewChecksum{
					Name:            object.Name,
					Namespace:       object.Namespace,
					ResourceVersion: object.ResourceVersion,
					Checksum:        object.Checksum,
					Kind:            kind,
					Event:           &event,
				})
			}

			err = multierr.Append(err, callbacks.Batch(ctx, *kind, domain.ReconciliationBatch, items))
		}
		if err != nil {
			return fmt.Errorf("failed to handle ReconciliationRequest message: %w", err)
		}
	case messaging.MsgPropEventValueGetObjectMessage:
		var data messaging.GetObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := callbacks.GetObject(ctx, domain.KindName{
			Kind:            domain.KindFromString(ctx, data.Kind),
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.BaseObject); err != nil {
			return fmt.Errorf("failed to send GetObject message: %w", err)
		}
	case messaging.MsgPropEventValuePatchObjectMessage:
		var data messaging.PatchObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := callbacks.PatchObject(ctx, domain.KindName{
			Kind:            domain.KindFromString(ctx, data.Kind),
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.Checksum, data.Patch); err != nil {
			return fmt.Errorf("failed to send PatchObject message: %w", err)
		}
	case messaging.MsgPropEventValueVerifyObjectMessage:
		var data messaging.VerifyObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := callbacks.VerifyObject(ctx, domain.KindName{
			Kind:            domain.KindFromString(ctx, data.Kind),
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.Checksum); err != nil {
			return fmt.Errorf("failed to send VerifyObject message: %w", err)
		}
	case messaging.MsgPropEventValuePutObjectMessage:
		var data messaging.PutObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := callbacks.PutObject(ctx, domain.KindName{
			Kind:            domain.KindFromString(ctx, data.Kind),
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.Object); err != nil {
			return fmt.Errorf("failed to send PutObject message: %w", err)
		}
	case messaging.MsgPropEventValueDeleteObjectMessage:
		var data messaging.DeleteObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := callbacks.DeleteObject(ctx, domain.KindName{
			Kind:            domain.KindFromString(ctx, data.Kind),
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}); err != nil {
			return fmt.Errorf("failed to send DeleteObject message: %w", err)
		}
	default:
		return fmt.Errorf("unknown message type")
	}

	return nil
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

func (p *PulsarMessageProducer) ProduceMessage(ctx context.Context, id domain.ClientIdentifier, eventType string, payload []byte) error {
	producerMessage := newProducerMessage(SynchronizerServerProducerKey, id.Account, id.Cluster, eventType, payload)
	p.producer.SendAsync(ctx, producerMessage, logPulsarSyncAsyncErrors)
	return nil
}

func (p *PulsarMessageProducer) ProduceMessageWithoutIdentifier(ctx context.Context, eventType string, payload []byte) error {
	producerMessage := newProducerMessage(SynchronizerServerProducerKey, "", "", eventType, payload)
	p.producer.SendAsync(ctx, producerMessage, logPulsarSyncAsyncErrors)
	return nil
}

func logPulsarSyncAsyncErrors(msgID pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
	var metricLabels prometheus.Labels
	if err != nil {
		metricLabels = prometheus.Labels{prometheusStatusLabel: prometheusStatusLabelValueError}
		logger.L().Error("failed to send message to pulsar",
			helpers.Error(err),
			helpers.String("messageID", msgID.String()),
			helpers.Int("payloadBytes", len(message.Payload)),
			helpers.Interface("messageProperties", message.Properties))
	} else {
		metricLabels = prometheus.Labels{prometheusStatusLabel: prometheusStatusLabelValueSuccess}
		logger.L().Debug("successfully sent message to pulsar", helpers.String("messageID", msgID.String()), helpers.Interface("messageProperties", message.Properties))
	}

	pulsarProducerMessagesProducedCounter.With(metricLabels).Inc()
	if message != nil {
		pulsarProducerMessagePayloadBytesProducedCounter.With(metricLabels).Add(float64(len(message.Payload)))
	}
}

func newProducerMessage(producerMessageKey, account, cluster, eventType string, payload []byte) *pulsar.ProducerMessage {
	producerMessageProperties := map[string]string{
		messaging.MsgPropTimestamp: time.Now().Format(time.RFC3339Nano),
		messaging.MsgPropEvent:     eventType,
	}

	if account != "" {
		producerMessageProperties[messaging.MsgPropAccount] = account
	}

	if cluster != "" {
		producerMessageProperties[messaging.MsgPropCluster] = cluster
	}

	return &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: producerMessageProperties,
		Key:        producerMessageKey,
	}
}
