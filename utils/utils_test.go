package utils

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kinbiko/jsonassert"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

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
			in:   FileContent("testdata/pod.json"),
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

func TestRemoveManagedFields(t *testing.T) {
	tests := []struct {
		name string
		obj  *unstructured.Unstructured
		want []byte
	}{
		{
			name: "Remove fields from networkPolicy",
			obj:  FileToUnstructured("testdata/networkPolicy.json"),
			want: FileContent("testdata/networkPolicyCleaned.json"),
		},
		{
			name: "Do nothing if no managedFields",
			obj:  FileToUnstructured("testdata/pod.json"),
			want: FileContent("testdata/pod.json"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RemoveManagedFields(tt.obj)
			ja := jsonassert.New(t)
			b, err := json.Marshal(tt.obj.Object)
			assert.NoError(t, err)
			ja.Assertf(string(b), string(tt.want))
		})
	}
}

func TestRemoveSpecificFields(t *testing.T) {
	tests := []struct {
		name   string
		fields [][]string
		obj    *unstructured.Unstructured
		want   []byte
	}{
		{
			name:   "remove fields from node",
			fields: [][]string{{"status", "conditions"}},
			obj:    FileToUnstructured("testdata/node.json"),
			want:   FileContent("testdata/nodeCleaned.json"),
		},
		{
			name:   "remove no fields from pod",
			fields: [][]string{},
			obj:    FileToUnstructured("testdata/pod.json"),
			want:   FileContent("testdata/pod.json"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RemoveSpecificFields(tt.obj, tt.fields)
			assert.NoError(t, err)
			ja := jsonassert.New(t)
			b, err := json.Marshal(tt.obj.Object)
			assert.NoError(t, err)
			ja.Assertf(string(b), string(tt.want))
		})
	}
}

func TestGreaterOrEqualVersion(t *testing.T) {
	testCases := []struct {
		a        string
		b        string
		expected bool
	}{
		{"v0.0.2", "v0.0.1", true},
		{"v0.0.1", "v0.0.2", false},
		{"v0.0.1", "v0.0.1", true},
	}

	for _, tc := range testCases {
		result := GreaterOrEqualVersion(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("For version %s >= %s, expected %v but got %v", tc.a, tc.b, tc.expected, result)
		}
	}
}

func TestIsBatchMessageSupported(t *testing.T) {
	testCases := []struct {
		version  string
		expected bool
	}{
		{"", false},                // Empty version should return false
		{"v0.0.56", false},         // Version less than the minimum supported version should return false
		{"v0.0.57", true},          // Minimum supported version should return true
		{"v0.0.58", true},          // Version greater than the minimum supported version should return true
		{"v1.0.0", true},           // Version with a major version greater than 0 should return true
		{"v1.2.3", true},           // Version with a major version greater than 0 should return true
		{"invalid_version", false}, // Invalid version should return false
	}

	for _, tc := range testCases {
		result := IsBatchMessageSupported(tc.version)
		if result != tc.expected {
			t.Errorf("For version %s, expected %v but got %v", tc.version, tc.expected, result)
		}
	}
}
