package backend

import (
	"testing"
	"time"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromConfig_NoPulsarConfig(t *testing.T) {
	components, err := newFromConfig(config.Config{})
	require.NoError(t, err)
	assert.Nil(t, components)
}

func TestNewFromConfig_RegistersWithMessaging(t *testing.T) {
	components, err := messaging.NewFromConfig(config.Config{})
	require.NoError(t, err)
	assert.Nil(t, components)
}

func TestNewFromConfig_ExplicitPulsarType(t *testing.T) {
	// An explicit type: "pulsar" selects Pulsar just like an absent type; with no
	// pulsarConfig it falls through to the mock (nil components, no error).
	components, err := newFromConfig(config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{Type: "pulsar"},
		},
	})
	require.NoError(t, err)
	assert.Nil(t, components)
}

func TestNewFromConfig_UnknownType(t *testing.T) {
	_, err := newFromConfig(config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{Type: "rabbitmq"},
		},
	})
	require.Error(t, err)
}

// shortenKafkaPingBudget shrinks the startup retry budget so failure paths finish in seconds
func shortenKafkaPingBudget(t *testing.T) {
	t.Helper()
	retries, delay, timeout := kafkaPingRetries, kafkaPingRetryDelay, kafkaPingTimeout
	kafkaPingRetries, kafkaPingRetryDelay, kafkaPingTimeout = 0, 10*time.Millisecond, 2*time.Second
	t.Cleanup(func() {
		kafkaPingRetries, kafkaPingRetryDelay, kafkaPingTimeout = retries, delay, timeout
	})
}

// without the startup check a server pointed at unreachable brokers would run healthy and
// silently never sync
func TestNewFromConfig_KafkaUnreachableBrokers(t *testing.T) {
	shortenKafkaPingBudget(t)

	components, err := newFromConfig(config.Config{
		Backend: config.Backend{
			MessageQueue: &config.MessageQueueConfig{
				Type: "kafka",
				KafkaConfig: validKafkaConfig(func(k *config.KafkaConfig) {
					// nothing is listening here, so the ping cannot succeed
					k.BootstrapServers = []string{"127.0.0.1:1"}
				}),
			},
		},
	})
	require.Error(t, err)
	assert.Nil(t, components)
	assert.Contains(t, err.Error(), "failed to reach kafka brokers")
}

// validKafkaConfig returns an otherwise-valid Kafka config with one field mutated,
// so each test case isolates the single setting under test.
func validKafkaConfig(mutate func(*config.KafkaConfig)) *config.KafkaConfig {
	kafkaCfg := &config.KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		ProducerTopic:    "out",
		ConsumerTopic:    "in",
	}
	mutate(kafkaCfg)
	return kafkaCfg
}

func TestNewFromConfig_KafkaValidation(t *testing.T) {
	tests := []struct {
		name  string
		kafka *config.KafkaConfig
	}{
		{name: "missing kafkaConfig", kafka: nil},
		{name: "missing bootstrapServers", kafka: &config.KafkaConfig{ProducerTopic: "out", ConsumerTopic: "in"}},
		{name: "missing producerTopic", kafka: &config.KafkaConfig{BootstrapServers: []string{"localhost:9092"}, ConsumerTopic: "in"}},
		{name: "missing consumerTopic", kafka: &config.KafkaConfig{BootstrapServers: []string{"localhost:9092"}, ProducerTopic: "out"}},
		// securityProtocol is authoritative; the following are inconsistent configs.
		{name: "unknown securityProtocol", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.SecurityProtocol = "KERBEROS" })},
		{name: "sasl protocol without mechanism", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_PLAINTEXT"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
		{name: "sasl protocol with invalid mechanism", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_PLAINTEXT"
			k.SASLMechanism = "SCRAM-SHA-1"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
		{name: "sasl protocol without username", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_SSL"
			k.SASLMechanism = "SCRAM-SHA-512"
			k.SASLPassword = "hunter2"
		})},
		{name: "sasl protocol without password", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_SSL"
			k.SASLMechanism = "SCRAM-SHA-512"
			k.SASLUsername = "svc"
		})},
		{name: "sasl fields on non-sasl protocol", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SSL"
			k.SASLMechanism = "PLAIN"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
		// PLAIN would send the password in cleartext without TLS.
		{name: "plain mechanism without tls", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_PLAINTEXT"
			k.SASLMechanism = "PLAIN"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
		{name: "tlsCaCertPath on non-tls protocol", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.TLSCaCertPath = "/etc/kafka/ca.crt" })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newFromConfig(config.Config{
				Backend: config.Backend{
					MessageQueue: &config.MessageQueueConfig{Type: "kafka", KafkaConfig: tt.kafka},
				},
			})
			require.Error(t, err)
		})
	}
}

// TestValidateKafkaSecurity_Accepts covers the security configurations that must
// now pass validation (they were rejected while SASL/TLS was unimplemented).
func TestValidateKafkaSecurity_Accepts(t *testing.T) {
	tests := []struct {
		name  string
		kafka *config.KafkaConfig
	}{
		{name: "plaintext (empty protocol)", kafka: validKafkaConfig(func(k *config.KafkaConfig) {})},
		{name: "explicit plaintext", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.SecurityProtocol = "PLAINTEXT" })},
		{name: "lowercase protocol", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "sasl_ssl"
			k.SASLMechanism = "scram-sha-512"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
		{name: "ssl only", kafka: validKafkaConfig(func(k *config.KafkaConfig) { k.SecurityProtocol = "SSL" })},
		{name: "sasl plaintext scram", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_PLAINTEXT"
			k.SASLMechanism = "SCRAM-SHA-256"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
		// PLAIN is allowed once TLS encrypts the channel.
		{name: "sasl ssl plain", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_SSL"
			k.SASLMechanism = "PLAIN"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
		{name: "sasl ssl scram-256", kafka: validKafkaConfig(func(k *config.KafkaConfig) {
			k.SecurityProtocol = "SASL_SSL"
			k.SASLMechanism = "SCRAM-SHA-256"
			k.SASLUsername = "svc"
			k.SASLPassword = "hunter2"
		})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, validateKafkaSecurity(tt.kafka))
		})
	}
}
