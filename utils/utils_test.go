package utils

import (
	"context"
	"testing"

	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
)

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
