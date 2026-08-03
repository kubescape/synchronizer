package backend

import (
	"context"
	"encoding/json"
	"net"
	"sync"
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

// routingAdapter records the verdict its pod's reader reached for each identifier, so that
// "did not deliver" can be told apart from "never received": a reader that silently stopped
// consuming would otherwise satisfy the assertion.
type routingAdapter struct {
	*Adapter
	mu       sync.Mutex
	verdicts map[string]bool
}

func (a *routingAdapter) IsRelated(ctx context.Context, id domain.ClientIdentifier) bool {
	related := a.Adapter.IsRelated(ctx, id)
	a.mu.Lock()
	a.verdicts[id.String()] = related
	a.mu.Unlock()
	return related
}

// filtered reports whether this pod saw a record for id and deliberately dropped it.
func (a *routingAdapter) filtered(id domain.ClientIdentifier) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	related, seen := a.verdicts[id.String()]
	return seen && !related
}

// kafkaTestPod is one server instance: its own producer, reader (in its own consumer
// group) and backend adapter, with a single connected cluster.
type kafkaTestPod struct {
	ctx            context.Context
	id             domain.ClientIdentifier
	reader         *KafkaMessageReader
	backendAdapter *routingAdapter
	clientAdapter  *recordingClientAdapter
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

	backendAdapter := &routingAdapter{
		Adapter:  NewBackendAdapter(ctx, producer, cfg.Backend),
		verdicts: map[string]bool{},
	}
	reader.Start(ctx, backendAdapter)

	id := domain.ClientIdentifier{Account: account, Cluster: cluster}
	podCtx := context.WithValue(ctx, domain.ContextKeyClientIdentifier, id)

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

	// surface an internal failure here instead of leaving it to show up as a bare timeout
	go func() {
		if err := client.Start(podCtx); err != nil {
			t.Logf("synchronizer client for %s stopped: %v", cluster, err)
		}
	}()
	go func() {
		if err := server.Start(podCtx); err != nil {
			t.Logf("synchronizer server for %s stopped: %v", cluster, err)
		}
	}()

	// a pod only routes to clusters it has registered, so wait until this one is
	require.Eventually(t, func() bool {
		return backendAdapter.IsRelated(podCtx, id)
	}, 30*time.Second, 100*time.Millisecond, "cluster never registered with the backend adapter")

	return &kafkaTestPod{
		ctx:            podCtx,
		id:             id,
		reader:         reader,
		backendAdapter: backendAdapter,
		clientAdapter:  clientAdapter,
	}
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
	// one partition on purpose: under a shared consumer group a single partition goes to
	// exactly one pod, so the other is guaranteed to miss its message. More partitions would
	// let the two records land on the two pods by luck and let the regression pass.
	createKafkaTopic(t, ctx, broker, inTopic, 1)

	const account = "11111111-2222-3333-4444-555555555555"
	podA := startKafkaTestPod(t, ctx, broker, outTopic, inTopic, account, "cluster-a")
	podB := startKafkaTestPod(t, ctx, broker, outTopic, inTopic, account, "cluster-b")
	require.NotEqual(t, podA.reader.GroupID(), podB.reader.GroupID(), "pods must not share a consumer group")
	waitForGroupsStable(t, ctx, broker, podA.reader.GroupID(), podB.reader.GroupID())

	// backend -> cluster: one command per cluster. Addressing both is what makes this
	// deterministic — under a shared consumer group whichever pod loses the assignment never
	// delivers. Addressing one cluster would only fail when that pod happened to lose it.
	backendProducer, err := NewKafkaMessageProducer(kafkaTestConfig(broker, inTopic, inTopic))
	require.NoError(t, err)
	t.Cleanup(backendProducer.Close)

	for _, pod := range []*kafkaTestPod{podA, podB} {
		payload, err := json.Marshal(messaging.PutObjectMessage{
			Kind:   "/v1/configmaps",
			Name:   "routed-" + pod.id.Cluster,
			Object: []byte(`{"kind":"ConfigMap","metadata":{"name":"routed","resourceVersion":"1"}}`),
			Depth:  1,
		})
		require.NoError(t, err)
		require.NoError(t, backendProducer.ProduceMessage(ctx, pod.id,
			messaging.MsgPropEventValuePutObjectMessage, payload))
	}

	for _, pod := range []*kafkaTestPod{podA, podB} {
		select {
		case id := <-pod.clientAdapter.puts:
			assert.Equal(t, "routed-"+pod.id.Cluster, id.Name, "%s received a command addressed elsewhere", pod.id.Cluster)
		case <-time.After(60 * time.Second):
			t.Fatalf("%s never received the command addressed to it", pod.id.Cluster)
		}
	}

	// and each pod saw the other's record and deliberately dropped it, rather than never
	// having consumed it — the property a shared consumer group would break
	for _, pair := range []struct{ pod, other *kafkaTestPod }{{podA, podB}, {podB, podA}} {
		require.Eventually(t, func() bool {
			return pair.pod.backendAdapter.filtered(pair.other.id)
		}, 30*time.Second, 100*time.Millisecond,
			"%s never saw and filtered the record addressed to %s", pair.pod.id.Cluster, pair.other.id.Cluster)
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
