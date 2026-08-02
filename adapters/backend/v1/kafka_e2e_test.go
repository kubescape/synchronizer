package backend

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// recordingClientAdapter signals when the server pushes an object down to the cluster.
// a channel rather than the mock's Resources map, to avoid racing the synchronizer goroutine.
type recordingClientAdapter struct {
	*adapters.MockAdapter
	puts chan domain.KindName
}

func (a *recordingClientAdapter) PutObject(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	if err := a.MockAdapter.PutObject(ctx, id, checksum, object); err != nil {
		return err
	}
	select {
	case a.puts <- id:
	default:
	}
	return nil
}

// kafkaTestPod is one server instance: its own producer, reader (in its own consumer
// group) and backend adapter, with a single connected cluster.
type kafkaTestPod struct {
	ctx           context.Context
	cluster       string
	reader        *KafkaMessageReader
	clientAdapter *recordingClientAdapter
}

// startKafkaTestPod wires a pod to one cluster over net.Pipe, standing in for the websocket
// (same pattern as the core tests). returns once the cluster is registered with the adapter.
func startKafkaTestPod(t *testing.T, ctx context.Context, broker, outTopic, inTopic, account, cluster string) *kafkaTestPod {
	t.Helper()

	cfg := kafkaTestConfig(broker, outTopic, inTopic)
	producer, err := NewKafkaMessageProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	reader, err := NewKafkaMessageReader(cfg)
	require.NoError(t, err)
	t.Cleanup(reader.Close)

	backendAdapter := NewBackendAdapter(ctx, producer, cfg.Backend)
	reader.Start(ctx, backendAdapter)

	podCtx := context.WithValue(ctx, domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: account,
		Cluster: cluster,
	})

	clientConn, serverConn := net.Pipe()
	clientAdapter := &recordingClientAdapter{
		MockAdapter: adapters.NewMockAdapter(true),
		puts:        make(chan domain.KindName, 8),
	}
	client, err := core.NewSynchronizerClient(podCtx, []adapters.Adapter{clientAdapter}, clientConn,
		func() (net.Conn, error) { return clientConn, nil })
	require.NoError(t, err)
	server, err := core.NewSynchronizerServer(podCtx, []adapters.Adapter{backendAdapter}, serverConn)
	require.NoError(t, err)

	go func() { _ = client.Start(podCtx) }()
	go func() { _ = server.Start(podCtx) }()

	// a pod only routes to clusters it has registered, so wait until this one is
	require.Eventually(t, func() bool {
		return backendAdapter.IsRelated(podCtx, domain.ClientIdentifier{Account: account, Cluster: cluster})
	}, 30*time.Second, 100*time.Millisecond, "cluster never registered with the backend adapter")

	return &kafkaTestPod{ctx: podCtx, cluster: cluster, reader: reader, clientAdapter: clientAdapter}
}

// TestKafkaE2E_TwoPodsRouteToConnectedClients checks the multi-pod guarantee: every pod
// consumes the whole inbound topic in its own group, but routes only what is addressed to
// a cluster connected to it. a shared consumer group would silently break this.
func TestKafkaE2E_TwoPodsRouteToConnectedClients(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	broker := startRedpandaContainer(t, ctx)
	outTopic := "armo.kubescape.synchronizer.out"
	inTopic := "armo.kubescape.synchronizer.in"
	createKafkaTopic(t, ctx, broker, outTopic, 2)
	createKafkaTopic(t, ctx, broker, inTopic, 1)

	const account = "11111111-2222-3333-4444-555555555555"
	podA := startKafkaTestPod(t, ctx, broker, outTopic, inTopic, account, "cluster-a")
	podB := startKafkaTestPod(t, ctx, broker, outTopic, inTopic, account, "cluster-b")
	require.NotEqual(t, podA.reader.GroupID(), podB.reader.GroupID(), "pods must not share a consumer group")
	waitForGroupsStable(t, ctx, broker, podA.reader.GroupID(), podB.reader.GroupID())

	// backend -> cluster: a command addressed to cluster-a
	backendProducer, err := NewKafkaMessageProducer(kafkaTestConfig(broker, inTopic, inTopic))
	require.NoError(t, err)
	t.Cleanup(backendProducer.Close)

	object := []byte(`{"kind":"ConfigMap","metadata":{"name":"routed","resourceVersion":"1"}}`)
	payload, err := json.Marshal(messaging.PutObjectMessage{
		Kind:   "/v1/configmaps",
		Name:   "routed",
		Object: object,
		Depth:  1,
	})
	require.NoError(t, err)
	require.NoError(t, backendProducer.ProduceMessage(ctx,
		domain.ClientIdentifier{Account: account, Cluster: "cluster-a"},
		messaging.MsgPropEventValuePutObjectMessage, payload))

	select {
	case id := <-podA.clientAdapter.puts:
		assert.Equal(t, "routed", id.Name)
	case <-time.After(60 * time.Second):
		t.Fatal("cluster-a never received the command addressed to it")
	}

	// pod B fetched the same record, but must have dropped it at IsRelated
	select {
	case id := <-podB.clientAdapter.puts:
		t.Fatalf("cluster-b was sent a command addressed to cluster-a: %s", id.Name)
	case <-time.After(5 * time.Second):
	}

	// cluster -> backend: a change in cluster-b reaches the outbound topic, keyed and headed
	// for the event ingester. a plain consumer stands in for it.
	consumer := newKafkaTestConsumer(t, broker, outTopic)
	kindName := domain.KindName{
		Kind:      domain.KindFromString(ctx, "/v1/configmaps"),
		Name:      "from-cluster-b",
		Namespace: "default",
	}
	newObject := []byte(`{"kind":"ConfigMap","metadata":{"name":"from-cluster-b","resourceVersion":"1"}}`)
	require.NoError(t, podB.clientAdapter.TestCallPutOrPatch(podB.ctx, kindName, nil, newObject))

	record := pollRecordMatching(t, ctx, consumer, func(record *kgo.Record) bool {
		event, ok := headerByKey(record.Headers, messaging.MsgPropEvent)
		if !ok || string(event) != messaging.MsgPropEventValuePutObjectMessage {
			return false
		}
		name, ok := headerByKey(record.Headers, messaging.MsgPropResourceName)
		return ok && string(name) == "from-cluster-b"
	})

	// ordering per cluster depends on this key, so assert it explicitly
	assert.Equal(t, account+"/cluster-b", string(record.Key))
	cluster, ok := headerByKey(record.Headers, messaging.MsgPropCluster)
	require.True(t, ok)
	assert.Equal(t, "cluster-b", string(cluster))
	acct, ok := headerByKey(record.Headers, messaging.MsgPropAccount)
	require.True(t, ok)
	assert.Equal(t, account, string(acct))
}

// pollRecordMatching polls until a record satisfies match. the outbound topic also carries
// ServerConnected traffic, so taking the first record would assert against the wrong message.
func pollRecordMatching(t *testing.T, ctx context.Context, consumer *kgo.Client, match func(*kgo.Record) bool) *kgo.Record {
	t.Helper()

	pollCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		fetches := consumer.PollFetches(pollCtx)
		if pollCtx.Err() != nil {
			t.Fatal("timed out waiting for a matching record on the outbound topic")
		}
		require.NoError(t, fetches.Err())
		for _, record := range fetches.Records() {
			if match(record) {
				return record
			}
		}
	}
}
