package backend

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	prometheusStatusLabel    = "status"
	prometheusKindLabel      = "kind"
	prometheusAccountLabel   = "account"
	prometheusClusterLabel   = "cluster"
	prometheusEventTypeLabel = "event_type"

	prometheusStatusLabelValueSuccess = "success"
	prometheusStatusLabelValueError   = "error"
)

var (
	connectedClientsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "synchronizer_connected_clients_count",
		Help: "The number of connected clients",
	})
	clientDisconnectionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_client_disconnection_count",
		Help: "Counter of client disconnections",
	}, []string{prometheusAccountLabel, prometheusClusterLabel})
	pulsarProducerMessagesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_messages_produced_count",
		Help: "The total number of messages produced to pulsar",
	}, []string{prometheusStatusLabel, prometheusEventTypeLabel, prometheusKindLabel, prometheusAccountLabel, prometheusClusterLabel})
	pulsarProducerMessagePayloadBytesProducedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "synchronizer_pulsar_producer_message_payload_bytes_produced_count",
		Help: "Counter of bytes published to pulsar (message payload) successfully",
	}, []string{prometheusStatusLabel, prometheusEventTypeLabel, prometheusKindLabel, prometheusAccountLabel, prometheusClusterLabel})
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
