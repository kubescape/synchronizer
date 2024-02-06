package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientIdentifier_String(t *testing.T) {
	type fields struct {
		Account string
		Cluster string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "account and cluster",
			fields: fields{
				Account: "account",
				Cluster: "cluster",
			},
			want: "account/cluster",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ClientIdentifier{
				Account: tt.fields.Account,
				Cluster: tt.fields.Cluster,
			}
			got := c.String()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestKindName_String(t *testing.T) {
	type fields struct {
		Kind            *Kind
		Name            string
		Namespace       string
		ResourceVersion int
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "kind, name and namespace",
			fields: fields{
				Kind: &Kind{
					Group:    "apps",
					Version:  "v1",
					Resource: "deployments",
				},
				Name:      "name",
				Namespace: "namespace",
			},
			want: "apps/v1/deployments/namespace/name",
		},
		{
			name: "empty kind",
			fields: fields{
				Name:      "name",
				Namespace: "namespace",
			},
			want: "/namespace/name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := KindName{
				Kind:            tt.fields.Kind,
				Name:            tt.fields.Name,
				Namespace:       tt.fields.Namespace,
				ResourceVersion: tt.fields.ResourceVersion,
			}
			if got := c.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
