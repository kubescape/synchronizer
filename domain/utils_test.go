package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKind_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "apps/v1/deployments",
		},
		{
			name: "/v1/pods",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := KindFromString(context.TODO(), tt.name)
			got := k.String()
			assert.Equal(t, tt.name, got)
		})
	}
}
