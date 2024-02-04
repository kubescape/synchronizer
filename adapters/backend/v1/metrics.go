package backend

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	prometheusStatusLabel = "status"

	prometheusStatusLabelValueSuccess = "success"
	prometheusStatusLabelValueError   = "error"
)

var (
	connectedClientsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "synchronizer_connected_clients_count",
		Help: "The number of connected clients",
	})
	pulsarProducerMessagesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_messages_produced_count",
		Help: "The total number of messages produced to pulsar",
	}, []string{prometheusStatusLabel})
	pulsarProducerMessagePayloadBytesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_message_payload_bytes_produced_count",
		Help: "Counter of bytes published to pulsar (message payload) successfully",
	}, []string{prometheusStatusLabel})
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
