package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
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
	broker, _ := runRedpandaContainer(t, ctx)
	return broker
}

// runRedpandaContainer also returns a terminate func, for tests that end the broker early.
// It is idempotent and shared with the registered cleanup, so termination stays asserted
// for every test while a caller can still tear the broker down mid-test. Calling
// Terminate twice is not an option: it closes the docker client on the way out.
func runRedpandaContainer(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) (string, func()) {
	t.Helper()

	opts = append([]testcontainers.ContainerCustomizer{
		// Raise the broker-wide batch limit so 64 MB payloads are accepted.
		redpanda.WithBootstrapConfig("kafka_batch_max_bytes", kafkaTestMaxMessageBytes),
	}, opts...)

	ctr, err := redpanda.Run(ctx, redpandaTestImage, opts...)
	require.NoError(t, err)

	var once sync.Once
	terminate := func() {
		once.Do(func() {
			if err := ctr.Terminate(context.Background()); err != nil {
				t.Errorf("failed to terminate redpanda container: %v", err)
			}
		})
	}
	t.Cleanup(terminate)

	broker, err := ctr.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return broker, terminate
}

// withPinnedKafkaPort binds the kafka API to a fixed host port on the loopback interface.
// The advertised listener is baked from the mapped port at first start, so a replaced broker
// must reuse that port.
func withPinnedKafkaPort(hostPort int) testcontainers.ContainerCustomizer {
	return testcontainers.WithHostConfigModifier(func(hostConfig *dockercontainer.HostConfig) {
		hostConfig.PortBindings = dockernetwork.PortMap{
			dockernetwork.MustParsePort("9092/tcp"): []dockernetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(hostPort)}},
		}
	})
}

// freeKafkaHostPort asks the kernel for an unused port and releases it, so the broker can be
// rebuilt on the same address. Racy in principle, but against the whole machine rather than
// against a hardcoded range that overlaps the pulsar tests'.
func freeKafkaHostPort(t *testing.T, ctx context.Context) int {
	t.Helper()
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
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

// newBackendTestProducer stands in for a real backend producer on the inbound topic. The
// server's own producer stamps the self-produced header and the reader drops those, so it
// can no longer be used to inject input on the topic under test.
func newBackendTestProducer(t *testing.T, broker, topic string) *kgo.Client {
	t.Helper()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(kafkaTestMaxMessageBytes),
		kgo.BrokerMaxWriteBytes(kafkaTestMaxMessageBytes),
	)
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	return producer
}

// produceBackendMessage writes a backend -> cluster message the way it really arrives: the
// same key and headers the server writes, without the self-produced source, which is what
// NewProducerMessage does on the pulsar side. Returns the error so a caller can retry.
func produceBackendMessage(ctx context.Context, producer *kgo.Client, id domain.ClientIdentifier, eventType string, payload []byte) error {
	properties := messaging.BuildProducerProperties(id.Account, id.Cluster, eventType)
	delete(properties, messaging.MsgPropProducerSource)

	return producer.ProduceSync(ctx, &kgo.Record{
		Key:     kafkaPartitionKey(id.Account, id.Cluster),
		Value:   payload,
		Headers: kafkaHeadersFromProperties(properties),
	}).FirstErr()
}

func TestNewFromConfig_WithKafka(t *testing.T) {
	requireIntegration(t)
	ctx := context.Background()
	broker := startRedpandaContainer(t, ctx)

	cfg := kafkaTestConfig(broker, "armo.kubescape.synchronizer.out", "armo.kubescape.synchronizer.in")
	// strict mode must still accept topics that do exist, and this test creates both
	cfg.Backend.MessageQueue.KafkaConfig.RequireTopicsExist = true
	createKafkaTopic(t, ctx, broker, cfg.Backend.MessageQueue.KafkaConfig.ProducerTopic, 1)
	createKafkaTopic(t, ctx, broker, cfg.Backend.MessageQueue.KafkaConfig.ConsumerTopic, 1)

	components, err := messaging.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, components)
	require.NotNil(t, components.Producer)
	require.NotNil(t, components.Reader)
	require.NotNil(t, components.Close)
	t.Cleanup(components.Close)
}

// the startup topic check must not turn "topic not created yet" into a startup failure:
// brokers may auto-create on first produce, and the operator may create them out of band.
// this pins the default, requireTopicsExist opts out of it, see below.
func TestNewFromConfig_KafkaToleratesMissingTopics(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)

	components, err := messaging.NewFromConfig(ctx, kafkaTestConfig(broker, "not.created.yet.out", "not.created.yet.in"))
	require.NoError(t, err)
	require.NotNil(t, components)
	t.Cleanup(components.Close)

	// and the check itself must not have created them: describing a topic is a read, and a
	// startup probe that silently provisions topics would hide a misconfigured topic name
	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer admClient.Close()
	topics, err := kadm.NewClient(admClient).ListTopics(ctx)
	require.NoError(t, err)
	assert.NotContains(t, topics, "not.created.yet.out")
	assert.NotContains(t, topics, "not.created.yet.in")
}

// with auto-creation off, tolerating a missing topic hands back the failure this whole check
// exists to prevent: the server comes up, answers /healthz, and never syncs
func TestNewFromConfig_KafkaRequireTopicsExistFailsStartup(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)

	cfg := kafkaTestConfig(broker, "not.created.yet.out", "not.created.yet.in")
	cfg.Backend.MessageQueue.KafkaConfig.RequireTopicsExist = true

	components, err := messaging.NewFromConfig(ctx, cfg)
	require.Error(t, err)
	assert.Nil(t, components)
	// the producer topic is checked first, so it is the one named
	assert.Contains(t, err.Error(), "not.created.yet.out")
	assert.Contains(t, err.Error(), "requireTopicsExist")
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

	cfg := kafkaTestConfig(broker, "armo.kubescape.synchronizer.out", inTopic)
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

	producer := newBackendTestProducer(t, broker, inTopic)

	// The reader starts at latest, so wait for it to join its group before producing,
	// otherwise the message lands before the offset settles and is missed.
	waitForGroupsStable(t, ctx, broker, reader.GroupID())

	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "reader-test", Depth: 1})
	require.NoError(t, err)
	require.NoError(t, produceBackendMessage(ctx, producer, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
		messaging.MsgPropEventValuePutObjectMessage, payload))

	select {
	case id := <-received:
		assert.Equal(t, "reader-test", id.Name)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for reader to dispatch the message")
	}
}

// the startup guard cannot see an alias feeding the server's own output back onto the topic
// it consumes, which would send messages straight back to the clusters they came from
func TestKafkaMessageReader_SkipsSelfProducedMessages(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)
	inTopic := "armo.kubescape.synchronizer.in"
	outTopic := "armo.kubescape.synchronizer.out"
	// one partition, so the two records below are strictly ordered
	createKafkaTopic(t, ctx, broker, inTopic, 1)

	cfg := kafkaTestConfig(broker, outTopic, inTopic)
	// one partition only orders the fetch; a single worker carries that order through to
	// the adapter, so the assertion below does not ride on defaultKafkaConsumerWorkers
	cfg.Backend.ConsumerWorkers = 1
	reader, err := NewKafkaMessageReader(cfg)
	require.NoError(t, err)
	t.Cleanup(reader.Close)

	received := make(chan string, 4)
	adapter := adapters.NewMockAdapter(false)
	adapter.RegisterCallbacks(ctx, domain.Callbacks{
		PutObject: func(_ context.Context, id domain.KindName, _ string, _ []byte) error {
			received <- id.Name
			return nil
		},
	})

	readerCtx, readerCancel := context.WithCancel(ctx)
	defer readerCancel()
	reader.Start(readerCtx, adapter)
	waitForGroupsStable(t, ctx, broker, reader.GroupID())

	id := domain.ClientIdentifier{Account: "account", Cluster: "cluster"}
	selfPayload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "self-produced", Depth: 1})
	require.NoError(t, err)
	backendPayload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "from-backend", Depth: 1})
	require.NoError(t, err)

	// the real producer, pointed at the consumed topic, so this asserts against the headers
	// production actually writes and not a hand-built copy of them
	selfProducer, err := NewKafkaMessageProducer(kafkaTestConfig(broker, inTopic, outTopic))
	require.NoError(t, err)
	t.Cleanup(selfProducer.Close)
	require.NoError(t, selfProducer.ProduceMessage(ctx, id, messaging.MsgPropEventValuePutObjectMessage, selfPayload))
	// ProduceMessage is fire-and-forget, and kafka only orders within a single client
	require.NoError(t, selfProducer.client.Flush(ctx))

	// same key and partition, produced second: it cannot arrive first unless the self-produced
	// record was skipped, which beats a timeout that a reader stuck on nothing would also pass
	require.NoError(t, produceBackendMessage(ctx, newBackendTestProducer(t, broker, inTopic), id,
		messaging.MsgPropEventValuePutObjectMessage, backendPayload))

	select {
	case name := <-received:
		assert.Equal(t, "from-backend", name, "a self-produced record must not reach the adapter")
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for the backend record")
	}
	select {
	case name := <-received:
		t.Fatalf("an extra message reached the adapter: %q", name)
	case <-time.After(2 * time.Second):
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
		cfg := kafkaTestConfig(broker, "armo.kubescape.synchronizer.out", inTopic)
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

	producer := newBackendTestProducer(t, broker, inTopic)

	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "fanout", Depth: 1})
	require.NoError(t, err)
	require.NoError(t, produceBackendMessage(ctx, producer, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
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
	hostPort := freeKafkaHostPort(t, ctx)
	broker, terminateBroker := runRedpandaContainer(t, ctx, withPinnedKafkaPort(hostPort))

	inTopic := "armo.kubescape.synchronizer.in"
	createKafkaTopic(t, ctx, broker, inTopic, 1)

	cfg := kafkaTestConfig(broker, "armo.kubescape.synchronizer.out", inTopic)
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

	producer := newBackendTestProducer(t, broker, inTopic)

	// prove the pipeline works before the outage
	produceUntilReceived(t, ctx, producer, received, "before-restart")

	// replace the broker rather than Stop/Start it: redpanda rewrites redpanda.yaml on first
	// boot, dropping the marker the testcontainers entrypoint waits for, so a restart hangs
	terminateBroker()
	brokerAfter, _ := runRedpandaContainer(t, ctx, withPinnedKafkaPort(hostPort))
	require.Equal(t, broker, brokerAfter, "broker must return on the same address")
	createKafkaTopic(t, ctx, brokerAfter, inTopic, 1)

	// the reader is at-most-once, so messages sent during the outage are not expected to
	// survive it. what must hold is that new traffic flows again: resilience, not durability.
	produceUntilReceived(t, ctx, producer, received, "after-restart")
}

// produceUntilReceived produces until the reader hands a message to its adapter. it retries
// because the reader consumes from the end, so anything sent before it (re)joins is missed.
func produceUntilReceived(t *testing.T, ctx context.Context, producer *kgo.Client, received <-chan string, name string) {
	t.Helper()

	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: name, Depth: 1})
	require.NoError(t, err)

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		// bound every attempt: ProduceSync blocks until the record is acked, so one call
		// against a broker that is down would otherwise outlast the retry deadline itself
		produceCtx, cancelProduce := context.WithTimeout(ctx, 10*time.Second)
		err := produceBackendMessage(produceCtx, producer, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
			messaging.MsgPropEventValuePutObjectMessage, payload)
		cancelProduce()
		if err != nil {
			// the broker is replaced mid-test: the loop is the assertion, not one attempt
			t.Logf("produce failed while waiting for %q, retrying: %v", name, err)
		}
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
