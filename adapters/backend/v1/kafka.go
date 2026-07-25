package backend

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	kafkaBackend = "kafka"

	defaultKafkaGroupIDPrefix   = "synchronizer-server"
	defaultKafkaMaxMessageBytes = 64 * 1024 * 1024 // 64 MB
	defaultKafkaConsumerWorkers = 10

	// kafkaProducerFlushTimeout bounds how long shutdown waits for buffered records.
	kafkaProducerFlushTimeout = 30 * time.Second

	// franz-go defaults BrokerMax{Write,Read}Bytes to Kafka's socket.request.max.bytes
	// (100 MB) and rejects a broker limit below the batch/fetch limit; 1 GB is its cap.
	kafkaDefaultBrokerBytes = 100 << 20
	kafkaMaxBrokerBytes     = 1 << 30
)

// newKafkaFromConfig creates the Kafka producer and reader from configuration.
func newKafkaFromConfig(cfg config.Config) (*messaging.Components, error) {
	kafkaCfg := cfg.Backend.MessageQueue.KafkaConfig
	if kafkaCfg == nil {
		return nil, fmt.Errorf("messageQueue.type is kafka but kafkaConfig is missing")
	}
	if len(kafkaCfg.BootstrapServers) == 0 {
		return nil, fmt.Errorf("kafkaConfig.bootstrapServers is required")
	}
	// An empty topic would fail every async produce inside franz-go, invisibly to callers.
	if kafkaCfg.ProducerTopic == "" {
		return nil, fmt.Errorf("kafkaConfig.producerTopic is required")
	}
	if kafkaCfg.ConsumerTopic == "" {
		return nil, fmt.Errorf("kafkaConfig.consumerTopic is required")
	}
	// SASL/TLS settings are validated in kafkaSecurityOptions when the clients are built.

	logger.L().Info("initializing kafka client",
		helpers.Interface("bootstrapServers", kafkaCfg.BootstrapServers))

	producer, err := NewKafkaMessageProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	reader, err := NewKafkaMessageReader(cfg)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("failed to create kafka reader: %w", err)
	}

	logger.L().Info("kafka message queue initialized",
		helpers.String("producerTopic", kafkaCfg.ProducerTopic),
		helpers.String("consumerTopic", kafkaCfg.ConsumerTopic))

	return &messaging.Components{
		Producer: producer,
		Reader:   reader,
		Close: func() {
			producer.Close()
			reader.Close()
		},
	}, nil
}

// kafkaCompressionCodec maps the configured compression name to a franz-go codec.
// It defaults to ZSTD to match the Pulsar producer.
func kafkaCompressionCodec(compressionType string) kgo.CompressionCodec {
	switch strings.ToLower(compressionType) {
	case "none":
		return kgo.NoCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	default:
		return kgo.ZstdCompression()
	}
}

func kafkaMaxMessageBytes(cfg *config.KafkaConfig) int {
	if cfg.MaxMessageBytes > 0 {
		return cfg.MaxMessageBytes
	}
	return defaultKafkaMaxMessageBytes
}

// kafkaRecordByteLimit returns the batch/fetch byte limit, clamped to avoid an
// int32 overflow and to stay within the broker limit.
func kafkaRecordByteLimit(maxMessageBytes int) int32 {
	if maxMessageBytes > kafkaMaxBrokerBytes {
		return kafkaMaxBrokerBytes
	}
	return int32(maxMessageBytes)
}

// kafkaBrokerByteLimit returns the BrokerMax{Write,Read}Bytes value, raised so the
// broker limit never sits below the configured per-message limit.
func kafkaBrokerByteLimit(maxMessageBytes int) int32 {
	limit := maxMessageBytes
	if limit < kafkaDefaultBrokerBytes {
		limit = kafkaDefaultBrokerBytes
	}
	if limit > kafkaMaxBrokerBytes {
		limit = kafkaMaxBrokerBytes
	}
	return int32(limit)
}

// ******************************
// * Kafka Message Producer  *//
// ******************************

type KafkaMessageProducer struct {
	client *kgo.Client
}

var _ messaging.MessageProducer = (*KafkaMessageProducer)(nil)

func NewKafkaMessageProducer(cfg config.Config) (*KafkaMessageProducer, error) {
	kafkaCfg := cfg.Backend.MessageQueue.KafkaConfig
	maxMessageBytes := kafkaMaxMessageBytes(kafkaCfg)

	securityOpts, err := kafkaSecurityOptions(kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build kafka producer security options: %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(kafkaCfg.BootstrapServers...),
		kgo.DefaultProduceTopic(kafkaCfg.ProducerTopic),
		kgo.ProducerBatchCompression(kafkaCompressionCodec(kafkaCfg.CompressionType)),
		kgo.ProducerBatchMaxBytes(kafkaRecordByteLimit(maxMessageBytes)),
		kgo.BrokerMaxWriteBytes(kafkaBrokerByteLimit(maxMessageBytes)),
	}
	opts = append(opts, securityOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer client: %w", err)
	}

	return &KafkaMessageProducer{client: client}, nil
}

func (p *KafkaMessageProducer) ProduceMessage(ctx context.Context, id domain.ClientIdentifier, eventType string, payload []byte, optionalProperties ...map[string]string) error {
	properties := messaging.BuildProducerProperties(id.Account, id.Cluster, eventType, optionalProperties...)
	record := &kgo.Record{
		Key:     kafkaPartitionKey(id.Account, id.Cluster),
		Value:   payload,
		Headers: kafkaHeadersFromProperties(properties),
	}
	p.client.Produce(ctx, record, logKafkaProduceResult)
	return nil
}

func (p *KafkaMessageProducer) ProduceMessageWithoutIdentifier(ctx context.Context, eventType string, payload []byte) error {
	properties := messaging.BuildProducerProperties("", "", eventType)
	record := &kgo.Record{
		Value:   payload,
		Headers: kafkaHeadersFromProperties(properties),
	}
	p.client.Produce(ctx, record, logKafkaProduceResult)
	return nil
}

func (p *KafkaMessageProducer) Close() {
	// franz-go's Close fails buffered records rather than flushing them, so flush
	// first to avoid dropping in-flight messages on shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), kafkaProducerFlushTimeout)
	defer cancel()
	if err := p.client.Flush(ctx); err != nil {
		logger.L().Error("failed to flush kafka producer before close", helpers.Error(err))
	}
	p.client.Close()
}

// kafkaPartitionKey returns the {account}/{cluster} partition key that keeps a
// cluster's events ordered. It returns nil when no identifier is set.
func kafkaPartitionKey(account, cluster string) []byte {
	if account == "" && cluster == "" {
		return nil
	}
	return []byte(account + "/" + cluster)
}

// kafkaHeadersFromProperties converts the property map to Kafka record headers.
// Values are UTF-8 encoded and empty values are omitted, not written as empty.
func kafkaHeadersFromProperties(properties map[string]string) []kgo.RecordHeader {
	headers := make([]kgo.RecordHeader, 0, len(properties))
	for key, value := range properties {
		if value == "" {
			continue
		}
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	return headers
}

func logKafkaProduceResult(record *kgo.Record, err error) {
	var messageProperties map[string]string
	var messagePayloadBytes int
	if record != nil {
		messagePayloadBytes = len(record.Value)
		messageProperties = kafkaPropertiesFromHeaders(record.Headers)
	}

	status := "success"
	if err != nil {
		status = "error"
		logger.L().Error("failed to send message to kafka",
			helpers.Error(err),
			helpers.Int("payloadBytes", messagePayloadBytes),
			helpers.Interface("messageProperties", messageProperties))
	} else {
		logger.L().Debug("successfully sent message to kafka", helpers.Interface("messageProperties", messageProperties))
	}

	messaging.RecordProducerMessage(kafkaBackend, status, messageProperties, messagePayloadBytes)
}

// ******************************
// * Kafka Message Reader  *//
// ******************************

type KafkaMessageReader struct {
	name           string
	client         *kgo.Client
	messageChannel chan *kgo.Record
	wg             sync.WaitGroup
	workers        int
	handler        *messaging.MessageHandler
}

var _ messaging.MessageReader = (*KafkaMessageReader)(nil)

func NewKafkaMessageReader(cfg config.Config) (*KafkaMessageReader, error) {
	kafkaCfg := cfg.Backend.MessageQueue.KafkaConfig

	workers := defaultKafkaConsumerWorkers
	if cfg.Backend.ConsumerWorkers > 0 {
		workers = cfg.Backend.ConsumerWorkers
	}

	groupIDPrefix := kafkaCfg.GroupIDPrefix
	if groupIDPrefix == "" {
		groupIDPrefix = defaultKafkaGroupIDPrefix
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	// A unique per-pod group.id replicates the Pulsar Reader fan-out: every pod
	// consumes the full topic, starting at latest and never committing (at-most-once).
	groupID := fmt.Sprintf("%s-%s", groupIDPrefix, hostname)

	securityOpts, err := kafkaSecurityOptions(kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build kafka consumer security options: %w", err)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(kafkaCfg.BootstrapServers...),
		kgo.ConsumeTopics(kafkaCfg.ConsumerTopic),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(kafkaRecordByteLimit(kafkaMaxMessageBytes(kafkaCfg))),
		kgo.BrokerMaxReadBytes(kafkaBrokerByteLimit(kafkaMaxMessageBytes(kafkaCfg))),
	}
	opts = append(opts, securityOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer client: %w", err)
	}

	logger.L().Debug("creating new kafka reader",
		helpers.String("readerName", groupID),
		helpers.String("topic", kafkaCfg.ConsumerTopic))

	return &KafkaMessageReader{
		name:           groupID,
		client:         client,
		messageChannel: make(chan *kgo.Record, workers),
		workers:        workers,
		handler:        &messaging.MessageHandler{Name: groupID},
	}, nil
}

func (c *KafkaMessageReader) Start(mainCtx context.Context, adapter adapters.Adapter) {
	for w := 1; w <= c.workers; w++ {
		logger.L().Info("starting to listening on kafka message channel", helpers.Int("worker", w))
		c.wg.Add(1)
		go c.listenOnMessageChannel(mainCtx, adapter)
	}

	go func() {
		logger.L().Info("starting to read messages from kafka")
		c.readerLoop(mainCtx)
		// readerLoop is the sole writer, so it owns the close: closing it elsewhere
		// could race a concurrent send. This also signals the workers to drain and exit.
		logger.L().Info("closing kafka message channel")
		close(c.messageChannel)
		c.wg.Wait()
		logger.L().Info("kafka workers stopped")
	}()
}

func (c *KafkaMessageReader) Close() {
	c.client.Close()
}

// GroupID returns the unique per-pod consumer group id this reader joined.
func (c *KafkaMessageReader) GroupID() string {
	return c.name
}

func (c *KafkaMessageReader) readerLoop(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			logger.L().Ctx(ctx).Info("kafka reader loop exiting due to client close")
			return
		}
		if err := ctx.Err(); err != nil {
			logger.L().Ctx(ctx).Info("kafka reader loop exiting due to context cancellation")
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			logger.L().Ctx(ctx).Error("failed to fetch messages from kafka",
				helpers.Error(err),
				helpers.String("topic", topic),
				helpers.Int("partition", int(partition)))
		})

		fetches.EachRecord(func(record *kgo.Record) {
			select {
			case c.messageChannel <- record:
				logger.L().Ctx(ctx).Debug("kafka message enqueued", helpers.String("msgId", kafkaMessageID(record)))
			case <-ctx.Done():
			}
		})
	}
}

func (c *KafkaMessageReader) listenOnMessageChannel(ctx context.Context, adapter adapters.Adapter) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-c.messageChannel:
			if !ok {
				return
			}
			msgID := kafkaMessageID(record)
			incoming := messaging.IncomingMessage{
				ID:         msgID,
				Properties: kafkaPropertiesFromHeaders(record.Headers),
				Payload:    record.Value,
			}
			if err := c.handler.Handle(ctx, adapter, incoming); err != nil {
				logger.L().Ctx(ctx).Error("failed to handle message", helpers.Error(err), helpers.String("msgId", msgID))
			} else {
				logger.L().Ctx(ctx).Debug("message processed successfully", helpers.String("msgId", msgID))
			}
		}
	}
}

func kafkaMessageID(record *kgo.Record) string {
	return fmt.Sprintf("%s/%d/%d", record.Topic, record.Partition, record.Offset)
}

func kafkaPropertiesFromHeaders(headers []kgo.RecordHeader) map[string]string {
	properties := make(map[string]string, len(headers))
	for _, header := range headers {
		properties[header.Key] = string(header.Value)
	}
	return properties
}
