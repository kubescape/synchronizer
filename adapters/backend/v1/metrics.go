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
