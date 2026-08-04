package backend

import (
	"testing"

	"github.com/kubescape/synchronizer/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestIsSelfProducedKafkaRecord(t *testing.T) {
	tests := []struct {
		name   string
		record *kgo.Record
		want   bool
	}{
		{
			name: "produced by the synchronizer server",
			record: &kgo.Record{
				Headers: kafkaHeadersFromProperties(messaging.BuildProducerProperties(
					"account", "cluster", messaging.MsgPropEventValuePutObjectMessage)),
			},
			want: true,
		},
		{
			name: "produced by a backend service",
			record: &kgo.Record{
				Headers: []kgo.RecordHeader{
					{Key: messaging.MsgPropEvent, Value: []byte(messaging.MsgPropEventValuePutObjectMessage)},
				},
			},
			want: false,
		},
		{
			// the source has to match, not just be present: a backend service that names
			// itself is still backend traffic and still has to get through
			name: "producer source set to another service",
			record: &kgo.Record{
				Headers: []kgo.RecordHeader{
					{Key: messaging.MsgPropProducerSource, Value: []byte("event-ingester-service")},
				},
			},
			want: false,
		},
		{
			name:   "no headers at all",
			record: &kgo.Record{},
			want:   false,
		},
		{
			// the pulsar key fallback must not apply here, kafka puts the partition key in
			// that field. pins the choice to pass "" instead of string(record.Key)
			name:   "pulsar producer key as the partition key",
			record: &kgo.Record{Key: []byte(messaging.SynchronizerServerProducerKey)},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSelfProducedKafkaRecord(tt.record))
		})
	}
}
