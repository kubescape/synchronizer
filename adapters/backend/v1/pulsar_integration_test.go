package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	pulsarconfig "github.com/kubescape/messaging/pulsar/config"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
}

func startPulsarContainer(t *testing.T, ctx context.Context) (testcontainers.Container, string, string) {
	t.Helper()

	brokerPort := fmt.Sprintf("%d", 20000+rand.Intn(10000))
	adminPort := fmt.Sprintf("%d", 30000+rand.Intn(10000))

	pulsarC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apachepulsar/pulsar:3.0.3",
			Cmd:          []string{"bin/pulsar", "standalone"},
			ExposedPorts: []string{brokerPort + ":6650/tcp", adminPort + ":8080/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForExposedPort(),
				wait.ForHTTP("/admin/v2/clusters").WithPort("8080/tcp").WithResponseMatcher(func(r io.Reader) bool {
					respBytes, _ := io.ReadAll(r)
					return strings.Contains(string(respBytes), `["standalone"]`)
				}),
			),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pulsarC.Terminate(ctx)
	})

	pulsarURL, err := pulsarC.PortEndpoint(ctx, "6650", "pulsar")
	require.NoError(t, err)
	pulsarAdminURL, err := pulsarC.PortEndpoint(ctx, "8080", "http")
	require.NoError(t, err)

	return pulsarC, pulsarURL, pulsarAdminURL
}

func pulsarTestConfig(pulsarURL, pulsarAdminURL string) config.Config {
	return config.Config{
		Backend: config.Backend{
			Subscription:  fmt.Sprintf("synchronizer-server-test-%d", rand.Intn(100000)),
			ProducerTopic: "synchronizer",
			ConsumerTopic: "synchronizer",
			PulsarConfig: &pulsarconfig.PulsarConfig{
				URL:       pulsarURL,
				AdminUrl:  pulsarAdminURL,
				Tenant:    "armo",
				Namespace: "kubescape",
				Clusters:  []string{"standalone"},
			},
		},
	}
}

func TestNewFromConfig_WithPulsar(t *testing.T) {
	requireIntegration(t)
	ctx := context.Background()
	_, pulsarURL, pulsarAdminURL := startPulsarContainer(t, ctx)

	components, err := messaging.NewFromConfig(ctx, pulsarTestConfig(pulsarURL, pulsarAdminURL))
	require.NoError(t, err)
	require.NotNil(t, components)
	require.NotNil(t, components.Producer)
	require.NotNil(t, components.Reader)
	require.NotNil(t, components.Close)
	t.Cleanup(components.Close)
}

func TestPulsarMessageProducer_SetsProducerSourceHeader(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, pulsarURL, pulsarAdminURL := startPulsarContainer(t, ctx)
	cfg := pulsarTestConfig(pulsarURL, pulsarAdminURL)

	pulsarClient, err := pulsarconnector.NewClient(
		pulsarconnector.WithConfig(cfg.Backend.PulsarConfig),
		pulsarconnector.WithRetryAttempts(20),
		pulsarconnector.WithRetryMaxDelay(3*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(pulsarClient.Close)

	producer, err := NewPulsarMessageProducer(cfg, pulsarClient)
	require.NoError(t, err)

	consumer, err := pulsarClient.NewConsumer(
		pulsarconnector.WithTopic(cfg.Backend.ProducerTopic),
		pulsarconnector.WithSubscriptionName(fmt.Sprintf("test-consumer-%d", rand.Intn(100000))),
	)
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	payload, err := json.Marshal(messaging.PutObjectMessage{
		Kind:  "/v1/configmaps",
		Name:  "test",
		Depth: 1,
	})
	require.NoError(t, err)

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

	msg, err := consumer.Receive(ctx)
	require.NoError(t, err)

	assert.Equal(t, messaging.SynchronizerServerProducerKey, msg.Key())
	assert.Equal(t, messaging.MsgPropProducerSourceSynchronizerServer, msg.Properties()[messaging.MsgPropProducerSource])
	assert.Equal(t, messaging.MsgPropEventValuePutObjectMessage, msg.Properties()[messaging.MsgPropEvent])
	assert.Equal(t, "account", msg.Properties()[messaging.MsgPropAccount])
	assert.Equal(t, "cluster", msg.Properties()[messaging.MsgPropCluster])
}

func TestPulsarMessageReader_SkipsSelfProducedMessages(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, pulsarURL, pulsarAdminURL := startPulsarContainer(t, ctx)
	cfg := pulsarTestConfig(pulsarURL, pulsarAdminURL)

	pulsarClient, err := pulsarconnector.NewClient(
		pulsarconnector.WithConfig(cfg.Backend.PulsarConfig),
		pulsarconnector.WithRetryAttempts(20),
		pulsarconnector.WithRetryMaxDelay(3*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(pulsarClient.Close)

	producer, err := NewPulsarMessageProducer(cfg, pulsarClient)
	require.NoError(t, err)

	externalConsumer, err := pulsarClient.NewConsumer(
		pulsarconnector.WithTopic(cfg.Backend.ConsumerTopic),
		pulsarconnector.WithSubscriptionName(fmt.Sprintf("external-consumer-%d", rand.Intn(100000))),
	)
	require.NoError(t, err)
	t.Cleanup(externalConsumer.Close)

	payload, err := json.Marshal(messaging.PutObjectMessage{
		Kind:  "/v1/configmaps",
		Name:  "test",
		Depth: 1,
	})
	require.NoError(t, err)

	err = producer.ProduceMessage(ctx, domain.ClientIdentifier{
		Account: "account",
		Cluster: "cluster",
	}, messaging.MsgPropEventValuePutObjectMessage, payload)
	require.NoError(t, err)

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 10*time.Second)
	defer receiveCancel()
	msg, err := externalConsumer.Receive(receiveCtx)
	require.NoError(t, err)
	assert.True(t, messaging.IsSelfProducedMessage(msg.Properties(), msg.Key()))
}
