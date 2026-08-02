package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaTestMaxMessageBytes = 128 * 1024 * 1024 // 128 MB, headroom above the 64 MB payload test

func startRedpandaContainer(t *testing.T, ctx context.Context) string {
	t.Helper()
	_, broker := runRedpandaContainer(t, ctx)
	return broker
}

// runRedpandaContainer also returns the container, for tests that control its lifecycle
func runRedpandaContainer(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*redpanda.Container, string) {
	t.Helper()

	opts = append([]testcontainers.ContainerCustomizer{
		// Raise the broker-wide batch limit so 64 MB payloads are accepted.
		redpanda.WithBootstrapConfig("kafka_batch_max_bytes", kafkaTestMaxMessageBytes),
	}, opts...)

	ctr, err := redpanda.Run(ctx, redpandaTestImage, opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		// ignored: the restart test terminates its own container
		_ = ctr.Terminate(context.Background())
	})

	broker, err := ctr.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return ctr, broker
}

// withPinnedKafkaPort binds the kafka API to a fixed host port. The advertised listener is
// baked from the mapped port at first start, so a replaced broker must reuse that port.
func withPinnedKafkaPort(hostPort int) testcontainers.ContainerCustomizer {
	return testcontainers.WithHostConfigModifier(func(hostConfig *dockercontainer.HostConfig) {
		hostConfig.PortBindings = nat.PortMap{
			nat.Port("9092/tcp"): []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: strconv.Itoa(hostPort)}},
		}
	})
}

func kafkaTestConfig(broker, producerTopic, consumerTopic string) config.Config {
	return config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{
				Type: "kafka",
				KafkaConfig: &config.KafkaConfig{
					BootstrapServers: []string{broker},
					ProducerTopic:    producerTopic,
					ConsumerTopic:    consumerTopic,
					GroupIDPrefix:    fmt.Sprintf("synchronizer-server-test-%d", rand.Intn(100000)),
					MaxMessageBytes:  kafkaTestMaxMessageBytes,
				},
			},
		},
	}
}

// createKafkaTopic creates a topic with the given partition count and a raised
// max.message.bytes so the large-message test round-trips.
func createKafkaTopic(t *testing.T, ctx context.Context, broker, topic string, partitions int32) {
	t.Helper()

	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer admClient.Close()

	maxBytes := strconv.Itoa(kafkaTestMaxMessageBytes)
	_, err = kadm.NewClient(admClient).CreateTopic(ctx, partitions, 1, map[string]*string{
		"max.message.bytes": &maxBytes,
	}, topic)
	require.NoError(t, err)
}

func newKafkaTestConsumer(t *testing.T, broker, topic string) *kgo.Client {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(kafkaTestMaxMessageBytes),
		kgo.BrokerMaxReadBytes(kafkaTestMaxMessageBytes),
	)
	require.NoError(t, err)
	t.Cleanup(consumer.Close)
	return consumer
}

func TestNewFromConfig_WithKafka(t *testing.T) {
	requireIntegration(t)
	ctx := context.Background()
	broker := startRedpandaContainer(t, ctx)

	cfg := kafkaTestConfig(broker, "armo.kubescape.synchronizer.out", "armo.kubescape.synchronizer.in")
	createKafkaTopic(t, ctx, broker, cfg.Backend.MessageQueue.KafkaConfig.ProducerTopic, 1)
	createKafkaTopic(t, ctx, broker, cfg.Backend.MessageQueue.KafkaConfig.ConsumerTopic, 1)

	components, err := messaging.NewFromConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, components)
	require.NotNil(t, components.Producer)
	require.NotNil(t, components.Reader)
	require.NotNil(t, components.Close)
	t.Cleanup(components.Close)
}

func TestKafkaMessageProducer_SetsHeadersAndPartitionKey(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)
	topic := "armo.kubescape.synchronizer.out"
	createKafkaTopic(t, ctx, broker, topic, 1)

	cfg := kafkaTestConfig(broker, topic, "armo.kubescape.synchronizer.in")
	producer, err := NewKafkaMessageProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	consumer := newKafkaTestConsumer(t, broker, topic)

	payload, err := json.Marshal(messaging.PutObjectMessage{
		Kind:  "/v1/configmaps",
		Name:  "test",
		Depth: 1,
	})
	require.NoError(t, err)

	// cluster-scoped object: namespace is empty and must be omitted from headers.
	err = producer.ProduceMessage(ctx, domain.ClientIdentifier{
		Account: "account",
		Cluster: "cluster",
	}, messaging.MsgPropEventValuePutObjectMessage, payload,
		messaging.KindNameToCustomProperties(domain.KindName{
			Kind: domain.KindFromString(ctx, "/v1/configmaps"),
			Name: "test",
		}),
	)
	require.NoError(t, err)

	record := pollOneRecord(t, ctx, consumer)
	assert.Equal(t, "account/cluster", string(record.Key))

	headers := kafkaPropertiesFromHeaders(record.Headers)
	assert.Equal(t, messaging.MsgPropEventValuePutObjectMessage, headers[messaging.MsgPropEvent])
	assert.Equal(t, "account", headers[messaging.MsgPropAccount])
	assert.Equal(t, "cluster", headers[messaging.MsgPropCluster])
	assert.Equal(t, "configmaps", headers[messaging.MsgPropResourceKindResource])
	assert.NotEmpty(t, headers[messaging.MsgPropTimestamp])
	// absent optional metadata is omitted, not written empty
	_, hasNamespace := headerByKey(record.Headers, messaging.MsgPropResourceNamespace)
	assert.False(t, hasNamespace, "empty namespace header should be omitted")
}

func TestKafkaMessageProducer_PreservesOrderPerPartitionKey(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)
	topic := "armo.kubescape.synchronizer.out"
	createKafkaTopic(t, ctx, broker, topic, 6)

	cfg := kafkaTestConfig(broker, topic, "armo.kubescape.synchronizer.in")
	producer, err := NewKafkaMessageProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	consumer := newKafkaTestConsumer(t, broker, topic)

	id := domain.ClientIdentifier{Account: "account", Cluster: "cluster"}
	putPayload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "cm", Depth: 1})
	require.NoError(t, err)
	deletePayload, err := json.Marshal(messaging.DeleteObjectMessage{Kind: "/v1/configmaps", Name: "cm", Depth: 1})
	require.NoError(t, err)

	require.NoError(t, producer.ProduceMessage(ctx, id, messaging.MsgPropEventValuePutObjectMessage, putPayload))
	require.NoError(t, producer.ProduceMessage(ctx, id, messaging.MsgPropEventValueDeleteObjectMessage, deletePayload))

	records := pollRecords(t, ctx, consumer, 2)
	first, second := records[0], records[1]

	// Put-then-Delete for the same resource share the {account}/{cluster} key, so
	// they land on one partition, in order.
	assert.Equal(t, first.Partition, second.Partition, "same key must map to the same partition")
	firstHeaders := kafkaPropertiesFromHeaders(first.Headers)
	secondHeaders := kafkaPropertiesFromHeaders(second.Headers)
	assert.Equal(t, messaging.MsgPropEventValuePutObjectMessage, firstHeaders[messaging.MsgPropEvent])
	assert.Equal(t, messaging.MsgPropEventValueDeleteObjectMessage, secondHeaders[messaging.MsgPropEvent])
}

func TestKafkaMessageProducer_LargeMessage(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)
	topic := "armo.kubescape.synchronizer.out"
	createKafkaTopic(t, ctx, broker, topic, 1)

	cfg := kafkaTestConfig(broker, topic, "armo.kubescape.synchronizer.in")
	producer, err := NewKafkaMessageProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	consumer := newKafkaTestConsumer(t, broker, topic)

	// ~64 MB payload validates client + broker + topic sizing end to end.
	largeObject := make([]byte, 64*1024*1024)
	for i := range largeObject {
		largeObject[i] = byte('a' + i%26)
	}
	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "big", Object: largeObject})
	require.NoError(t, err)

	err = producer.ProduceMessage(ctx, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
		messaging.MsgPropEventValuePutObjectMessage, payload)
	require.NoError(t, err)

	record := pollOneRecord(t, ctx, consumer)
	assert.Equal(t, len(payload), len(record.Value))
}

func TestKafkaMessageReader_DispatchesToAdapter(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)
	inTopic := "armo.kubescape.synchronizer.in"
	createKafkaTopic(t, ctx, broker, inTopic, 1)

	// producerTopic points at the .in topic so we can inject a backend->cluster command.
	cfg := kafkaTestConfig(broker, inTopic, inTopic)
	reader, err := NewKafkaMessageReader(cfg)
	require.NoError(t, err)
	t.Cleanup(reader.Close)

	received := make(chan domain.KindName, 1)
	adapter := adapters.NewMockAdapter(false)
	adapter.RegisterCallbacks(ctx, domain.Callbacks{
		PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
			received <- id
			return nil
		},
	})

	readerCtx, readerCancel := context.WithCancel(ctx)
	defer readerCancel()
	reader.Start(readerCtx, adapter)

	producer, err := NewKafkaMessageProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	// The reader starts at latest, so wait for it to join its group before producing,
	// otherwise the message lands before the offset settles and is missed.
	waitForGroupsStable(t, ctx, broker, reader.GroupID())

	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "reader-test", Depth: 1})
	require.NoError(t, err)
	require.NoError(t, producer.ProduceMessage(ctx, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
		messaging.MsgPropEventValuePutObjectMessage, payload))

	select {
	case id := <-received:
		assert.Equal(t, "reader-test", id.Name)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for reader to dispatch the message")
	}
}

func TestKafkaMessageReader_FanOutAcrossGroups(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)
	inTopic := "armo.kubescape.synchronizer.in"
	createKafkaTopic(t, ctx, broker, inTopic, 1)

	// Two readers with distinct group.ids (unique GroupIDPrefix per config) must
	// each receive the full topic, replicating Pulsar Reader fan-out.
	var wg sync.WaitGroup
	received := make([]chan domain.KindName, 2)
	groupIDs := make([]string, 0, len(received))
	for i := range received {
		received[i] = make(chan domain.KindName, 1)
		cfg := kafkaTestConfig(broker, inTopic, inTopic)
		reader, err := NewKafkaMessageReader(cfg)
		require.NoError(t, err)
		t.Cleanup(reader.Close)
		groupIDs = append(groupIDs, reader.GroupID())

		ch := received[i]
		adapter := adapters.NewMockAdapter(false)
		adapter.RegisterCallbacks(ctx, domain.Callbacks{
			PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
				ch <- id
				return nil
			},
		})
		reader.Start(ctx, adapter)
	}

	// Wait for both readers to join their (distinct) groups before producing.
	waitForGroupsStable(t, ctx, broker, groupIDs...)

	producer, err := NewKafkaMessageProducer(kafkaTestConfig(broker, inTopic, inTopic))
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "fanout", Depth: 1})
	require.NoError(t, err)
	require.NoError(t, producer.ProduceMessage(ctx, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
		messaging.MsgPropEventValuePutObjectMessage, payload))

	for i := range received {
		wg.Add(1)
		go func(ch chan domain.KindName) {
			defer wg.Done()
			select {
			case id := <-ch:
				assert.Equal(t, "fanout", id.Name)
			case <-time.After(60 * time.Second):
				t.Error("timed out waiting for a reader group to receive the message")
			}
		}(received[i])
	}
	wg.Wait()
}

func TestKafkaMessageReader_SurvivesBrokerRestart(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// pin the port up front: the broker must come back at the same address to reconnect to
	hostPort := 26000 + rand.Intn(4000)
	ctr, broker := runRedpandaContainer(t, ctx, withPinnedKafkaPort(hostPort))

	inTopic := "armo.kubescape.synchronizer.in"
	createKafkaTopic(t, ctx, broker, inTopic, 1)

	cfg := kafkaTestConfig(broker, inTopic, inTopic)
	reader, err := NewKafkaMessageReader(cfg)
	require.NoError(t, err)
	t.Cleanup(reader.Close)

	received := make(chan string, 16)
	adapter := adapters.NewMockAdapter(false)
	adapter.RegisterCallbacks(ctx, domain.Callbacks{
		PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
			// non-blocking: a burst on reconnect would otherwise wedge a reader worker
			select {
			case received <- id.Name:
			default:
			}
			return nil
		},
	})
	reader.Start(ctx, adapter)
	waitForGroupsStable(t, ctx, broker, reader.GroupID())

	producer, err := NewKafkaMessageProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	// prove the pipeline works before the outage
	produceUntilReceived(t, ctx, producer, received, "before-restart")

	// replace the broker rather than Stop/Start it: redpanda rewrites redpanda.yaml on first
	// boot, dropping the marker the testcontainers entrypoint waits for, so a restart hangs
	require.NoError(t, ctr.Terminate(ctx))
	_, brokerAfter := runRedpandaContainer(t, ctx, withPinnedKafkaPort(hostPort))
	require.Equal(t, broker, brokerAfter, "broker must return on the same address")
	createKafkaTopic(t, ctx, brokerAfter, inTopic, 1)

	// the reader is at-most-once, so messages sent during the outage are not expected to
	// survive it. what must hold is that new traffic flows again: resilience, not durability.
	produceUntilReceived(t, ctx, producer, received, "after-restart")
}

// produceUntilReceived produces until the reader hands a message to its adapter. it retries
// because the reader consumes from the end, so anything sent before it (re)joins is missed.
func produceUntilReceived(t *testing.T, ctx context.Context, producer *KafkaMessageProducer, received <-chan string, name string) {
	t.Helper()

	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: name, Depth: 1})
	require.NoError(t, err)

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		require.NoError(t, producer.ProduceMessage(ctx, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
			messaging.MsgPropEventValuePutObjectMessage, payload))
		select {
		case got := <-received:
			if got == name {
				return
			}
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("timed out waiting for %q to reach the adapter", name)
}

// waitForGroupsStable polls until every given consumer group is Stable with at least
// one member, i.e. the readers have joined and been assigned partitions.
func waitForGroupsStable(t *testing.T, ctx context.Context, broker string, groupIDs ...string) {
	t.Helper()

	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer admClient.Close()
	adm := kadm.NewClient(admClient)

	require.Eventually(t, func() bool {
		described, err := adm.DescribeGroups(ctx, groupIDs...)
		if err != nil {
			return false
		}
		for _, groupID := range groupIDs {
			group, ok := described[groupID]
			if !ok || group.State != "Stable" || len(group.Members) == 0 {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond, "consumer groups did not become stable")
}

func pollOneRecord(t *testing.T, ctx context.Context, consumer *kgo.Client) *kgo.Record {
	t.Helper()
	return pollRecords(t, ctx, consumer, 1)[0]
}

// pollRecords accumulates exactly n records across fetches. A single PollFetches can
// return multiple records, so polling per-record would drop the extras it shares a fetch with.
func pollRecords(t *testing.T, ctx context.Context, consumer *kgo.Client, n int) []*kgo.Record {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	records := make([]*kgo.Record, 0, n)
	for len(records) < n {
		fetches := consumer.PollFetches(pollCtx)
		require.NoError(t, fetches.Err())
		records = append(records, fetches.Records()...)
	}
	return records
}

func headerByKey(headers []kgo.RecordHeader, key string) ([]byte, bool) {
	for _, header := range headers {
		if header.Key == key {
			return header.Value, true
		}
	}
	return nil, false
}
