package utils

import (
	"context"
	"os"
	"testing"

	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
)

func fileContent(path string) []byte {
	b, _ := os.ReadFile(path)
	return b
}

func TestCanonicalHash(t *testing.T) {
	tests := []struct {
		name    string
		in      []byte
		want    string
		wantErr bool
	}{
		{
			name:    "error",
			in:      []byte("test"),
			wantErr: true,
		},
		{
			name: "empty",
			in:   []byte("{}"),
			want: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "simple",
			in:   []byte(`{"a":"b"}`),
			want: "baf4fd048ca2e8f75d531af13c5869eaa8e38c3020e1dfcebe3c3ac019a3bab2",
		},
		{
			name: "pod",
			in:   fileContent("testdata/pod.json"),
			want: "1ae52b23166388144c602360fb73dd68736e88943f6e16fab1bf07347484f8e8",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CanonicalHash(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("CanonicalHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestContextFromGeneric(t *testing.T) {
	got := ContextFromGeneric(context.TODO(), domain.Generic{})
	assert.Equal(t, 0, got.Value(domain.ContextKeyDepth))
	assert.NotNil(t, got.Value(domain.ContextKeyMsgId))
}

func TestClientIdentifier_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		id   domain.ClientIdentifier
	}{
		{
			name: "empty",
			id:   domain.ClientIdentifier{},
		},
		{
			name: "with account",
			id: domain.ClientIdentifier{
				Account: "account",
			},
		},
		{
			name: "with cluster",
			id: domain.ClientIdentifier{
				Cluster: "cluster",
			},
		},
		{
			name: "with account and cluster",
			id: domain.ClientIdentifier{
				Account: "account",
				Cluster: "cluster",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := ContextFromIdentifiers(context.TODO(), tt.id)
			got := ClientIdentifierFromContext(ctx)
			assert.Equal(t, tt.id, got)
		})
	}
}
