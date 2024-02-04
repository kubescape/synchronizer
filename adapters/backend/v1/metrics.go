package backend

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	connectedClientsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "synchronizer_connected_clients_count",
		Help: "The number of connected clients",
	})
	pulsarProducerMessagesProducedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_messages_produced_count",
		Help: "The total number of messages produced to pulsar",
	})
	pulsarProducerMessagePayloadBytesProducedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_message_payload_bytes_produced_count",
		Help: "Counter of bytes published to pulsar (message payload)",
	})
	pulsarProducerErrorsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_errors_count",
		Help: "Counter of errors in the pulsar producer",
	})
	/*
		messagesReceivedCounter = promauto.NewCounter(prometheus.CounterOpts{
			Name: "synchronizer_messages_received_count",
			Help: "The total number of messages received",
		})

		messagesSentCounter = promauto.NewCounter(prometheus.CounterOpts{
			Name: "synchronizer_messages_sent_count",
			Help: "The total number of messages sent",
		})
	*/
)
