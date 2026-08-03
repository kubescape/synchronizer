package backend

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// The redpanda test module creates service accounts with SCRAM-SHA-256.
const (
	redpandaTestImage = "redpandadata/redpanda:v24.2.7"

	kafkaTestSASLUser      = "synchronizer"
	kafkaTestSASLPassword  = "synchronizer-secret"
	kafkaTestSASLMechanism = "SCRAM-SHA-256"
)

// startRedpandaSecure starts a broker with the given security options (topic
// auto-creation is always on) and returns its seed broker address.
func startRedpandaSecure(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) string {
	t.Helper()

	container, err := redpanda.Run(ctx, redpandaTestImage,
		append([]testcontainers.ContainerCustomizer{redpanda.WithAutoCreateTopics()}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	broker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return broker
}

// saslContainerOptions configures a broker to require SASL/SCRAM auth for the
// shared test service account.
func saslContainerOptions() []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		redpanda.WithEnableSASL(),
		redpanda.WithNewServiceAccount(kafkaTestSASLUser, kafkaTestSASLPassword),
		redpanda.WithSuperusers(kafkaTestSASLUser),
	}
}

// writeCACert writes a PEM certificate to a temp file and returns its path, so
// kafkaTLSConfig can load it as a trusted CA.
func writeCACert(t *testing.T, certPEM []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(path, certPEM, 0o600))
	return path
}

// generateSelfSignedCert returns a PEM cert/key valid for localhost and the
// loopback addresses, which is where testcontainers publishes the broker. The
// certificate is its own CA.
func generateSelfSignedCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// A random serial keeps two independently generated certs distinct (used by the
	// untrusted-broker test).
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM
}

// newKafkaSecureTestConsumer builds a raw consumer that reuses the adapter's own
// security wiring, so the test verifies the same SASL/TLS options as production.
func newKafkaSecureTestConsumer(t *testing.T, broker, topic string, kafkaCfg *config.KafkaConfig) *kgo.Client {
	t.Helper()

	securityOpts, err := kafkaSecurityOptions(kafkaCfg)
	require.NoError(t, err)

	opts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	opts = append(opts, securityOpts...)

	consumer, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(consumer.Close)
	return consumer
}

// createSecureKafkaTopic creates a topic through an admin client that reuses the
// adapter's security wiring, so it works against SASL/TLS brokers.
func createSecureKafkaTopic(t *testing.T, ctx context.Context, broker, topic string, kafkaCfg *config.KafkaConfig) {
	t.Helper()

	securityOpts, err := kafkaSecurityOptions(kafkaCfg)
	require.NoError(t, err)

	admClient, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(broker)}, securityOpts...)...)
	require.NoError(t, err)
	defer admClient.Close()

	_, err = kadm.NewClient(admClient).CreateTopic(ctx, 1, 1, nil, topic)
	require.NoError(t, err)
}

// produceAndAssertRoundTrip produces one PutObject message and asserts it is read
// back intact, exercising both the producer and a consumer over the given config.
func produceAndAssertRoundTrip(t *testing.T, ctx context.Context, broker, topic string, kafkaCfg *config.KafkaConfig) {
	t.Helper()

	createSecureKafkaTopic(t, ctx, broker, topic, kafkaCfg)

	cfg := config.Config{Backend: config.Backend{MessageQueue: &config.MessageQueueConfig{Type: "kafka", KafkaConfig: kafkaCfg}}}
	producer, err := NewKafkaMessageProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	consumer := newKafkaSecureTestConsumer(t, broker, topic, kafkaCfg)

	payload, err := json.Marshal(messaging.PutObjectMessage{Kind: "/v1/configmaps", Name: "secure", Depth: 1})
	require.NoError(t, err)

	require.NoError(t, producer.ProduceMessage(ctx, domain.ClientIdentifier{Account: "account", Cluster: "cluster"},
		messaging.MsgPropEventValuePutObjectMessage, payload))

	record := pollOneRecord(t, ctx, consumer)
	assert.Equal(t, "account/cluster", string(record.Key))
	headers := kafkaPropertiesFromHeaders(record.Headers)
	assert.Equal(t, messaging.MsgPropEventValuePutObjectMessage, headers[messaging.MsgPropEvent])
	assert.Equal(t, payload, record.Value)
}

func TestKafkaSecurity_SASLRoundTrip(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startRedpandaSecure(t, ctx, saslContainerOptions()...)
	topic := "armo.kubescape.synchronizer.out"

	kafkaCfg := &config.KafkaConfig{
		BootstrapServers: []string{broker},
		ProducerTopic:    topic,
		ConsumerTopic:    "armo.kubescape.synchronizer.in",
		GroupIDPrefix:    fmt.Sprintf("synchronizer-server-sasl-%d", time.Now().UnixNano()),
		SecurityProtocol: "SASL_PLAINTEXT",
		SASLMechanism:    kafkaTestSASLMechanism,
		SASLUsername:     kafkaTestSASLUser,
		SASLPassword:     kafkaTestSASLPassword,
	}
	require.NoError(t, validateKafkaSecurity(kafkaCfg))

	produceAndAssertRoundTrip(t, ctx, broker, topic, kafkaCfg)
}

// a rejected credential must fail startup, not leave a client that never authenticates
func TestKafkaSecurity_BadCredentialsFailStartup(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	shortenKafkaPingBudget(t)
	broker := startRedpandaSecure(t, ctx, saslContainerOptions()...)

	components, err := newFromConfig(ctx, config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{
				Type: "kafka",
				KafkaConfig: &config.KafkaConfig{
					BootstrapServers: []string{broker},
					ProducerTopic:    "armo.kubescape.synchronizer.out",
					ConsumerTopic:    "armo.kubescape.synchronizer.in",
					GroupIDPrefix:    fmt.Sprintf("synchronizer-server-badcreds-%d", time.Now().UnixNano()),
					SecurityProtocol: "SASL_PLAINTEXT",
					SASLMechanism:    kafkaTestSASLMechanism,
					SASLUsername:     kafkaTestSASLUser,
					SASLPassword:     "wrong-" + kafkaTestSASLPassword,
				},
			},
		},
	})
	require.Error(t, err)
	assert.Nil(t, components)
	assert.Contains(t, err.Error(), "failed to reach kafka brokers")
}

// credentials the broker accepts are not enough: a principal with no ACL on the topics
// authenticates fine and then silently never syncs, so startup must reject it too
func TestKafkaSecurity_UnauthorizedTopicFailsStartup(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	shortenKafkaPingBudget(t)
	// authorization has to be enabled explicitly; SASL alone only authenticates, after
	// which any authenticated principal may do anything
	broker := startRedpandaSecure(t, ctx,
		redpanda.WithEnableSASL(),
		redpanda.WithBootstrapConfig("kafka_enable_authorization", true),
		redpanda.WithNewServiceAccount(kafkaTestSASLUser, kafkaTestSASLPassword),
		redpanda.WithNewServiceAccount("admin", "admin-secret"),
		redpanda.WithSuperusers("admin"),
	)

	components, err := newFromConfig(ctx, config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{
				Type: "kafka",
				KafkaConfig: &config.KafkaConfig{
					BootstrapServers: []string{broker},
					ProducerTopic:    "armo.kubescape.synchronizer.out",
					ConsumerTopic:    "armo.kubescape.synchronizer.in",
					GroupIDPrefix:    fmt.Sprintf("synchronizer-server-noacl-%d", time.Now().UnixNano()),
					SecurityProtocol: "SASL_PLAINTEXT",
					SASLMechanism:    kafkaTestSASLMechanism,
					SASLUsername:     kafkaTestSASLUser,
					SASLPassword:     kafkaTestSASLPassword,
				},
			},
		},
	})
	require.Error(t, err)
	assert.Nil(t, components)
	assert.Contains(t, err.Error(), "armo.kubescape.synchronizer.out")
}

func TestKafkaSecurity_TLSRoundTrip(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	certPEM, keyPEM := generateSelfSignedCert(t)
	broker := startRedpandaSecure(t, ctx, redpanda.WithTLS(certPEM, keyPEM))
	topic := "armo.kubescape.synchronizer.out"

	kafkaCfg := &config.KafkaConfig{
		BootstrapServers: []string{broker},
		ProducerTopic:    topic,
		ConsumerTopic:    "armo.kubescape.synchronizer.in",
		GroupIDPrefix:    fmt.Sprintf("synchronizer-server-tls-%d", time.Now().UnixNano()),
		SecurityProtocol: "SSL",
		TLSCaCertPath:    writeCACert(t, certPEM),
	}
	require.NoError(t, validateKafkaSecurity(kafkaCfg))

	produceAndAssertRoundTrip(t, ctx, broker, topic, kafkaCfg)
}

// TestKafkaSecurity_SASLTLSRoundTrip exercises the enterprise path: SASL auth over
// a TLS-encrypted channel (SASL_SSL), as recommended for non-dev deployments.
func TestKafkaSecurity_SASLTLSRoundTrip(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	certPEM, keyPEM := generateSelfSignedCert(t)
	broker := startRedpandaSecure(t, ctx, append(saslContainerOptions(), redpanda.WithTLS(certPEM, keyPEM))...)
	topic := "armo.kubescape.synchronizer.out"

	kafkaCfg := &config.KafkaConfig{
		BootstrapServers: []string{broker},
		ProducerTopic:    topic,
		ConsumerTopic:    "armo.kubescape.synchronizer.in",
		GroupIDPrefix:    fmt.Sprintf("synchronizer-server-sasltls-%d", time.Now().UnixNano()),
		SecurityProtocol: "SASL_SSL",
		SASLMechanism:    kafkaTestSASLMechanism,
		SASLUsername:     kafkaTestSASLUser,
		SASLPassword:     kafkaTestSASLPassword,
		TLSCaCertPath:    writeCACert(t, certPEM),
	}
	require.NoError(t, validateKafkaSecurity(kafkaCfg))

	produceAndAssertRoundTrip(t, ctx, broker, topic, kafkaCfg)
}

// TestKafkaSecurity_TLSRejectsUntrustedBroker verifies TLS verification is actually
// enforced: a client that trusts a different CA than the broker presents must fail
// to connect (not silently skip verification).
func TestKafkaSecurity_TLSRejectsUntrustedBroker(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokerCert, brokerKey := generateSelfSignedCert(t)
	broker := startRedpandaSecure(t, ctx, redpanda.WithTLS(brokerCert, brokerKey))

	// Trust a CA unrelated to the broker's certificate; verification must reject it.
	untrustedCert, _ := generateSelfSignedCert(t)
	kafkaCfg := &config.KafkaConfig{
		BootstrapServers: []string{broker},
		ProducerTopic:    "armo.kubescape.synchronizer.out",
		ConsumerTopic:    "armo.kubescape.synchronizer.in",
		SecurityProtocol: "SSL",
		TLSCaCertPath:    writeCACert(t, untrustedCert),
	}

	securityOpts, err := kafkaSecurityOptions(kafkaCfg)
	require.NoError(t, err)
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(broker), kgo.RequestRetries(1)}, securityOpts...)...)
	require.NoError(t, err)
	defer client.Close()

	reqCtx, reqCancel := context.WithTimeout(ctx, 20*time.Second)
	defer reqCancel()
	_, err = kadm.NewClient(client).ListTopics(reqCtx)
	require.Error(t, err, "client must refuse a broker whose cert is signed by an untrusted CA")
}
