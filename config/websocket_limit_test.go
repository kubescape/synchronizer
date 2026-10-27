package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackendWebSocketMaxMessageBytes(t *testing.T) {
	kafka := &MessageQueueConfig{KafkaConfig: &KafkaConfig{MaxMessageBytes: 128 << 20}}
	assert.Equal(t, 0, Backend{}.WebSocketMaxMessageBytes(), "nothing configured leaves the core default")
	assert.Equal(t, 128<<20, Backend{MessageQueue: kafka}.WebSocketMaxMessageBytes(), "the Kafka limit carries over")
	assert.Equal(t, 96<<20, Backend{MaxMessageBytes: 96 << 20, MessageQueue: kafka}.WebSocketMaxMessageBytes(), "an explicit value wins")
}
