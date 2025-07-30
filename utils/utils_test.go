package utils

import (
	"context"
	"testing"

	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

func TestMaskEnvironmentVariables(t *testing.T) {
	// Test cases
	tests := []struct {
		name     string
		input    *unstructured.Unstructured
		expected *unstructured.Unstructured
	}{
		{
			name: "Deployment with env vars in main and init containers",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"spec": map[string]interface{}{
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name": "main-container",
										"env": []interface{}{
											map[string]interface{}{
												"name":  "SECRET_KEY",
												"value": "supersecretvalue",
											},
											map[string]interface{}{
												"name": "FROM_SECRET",
												"valueFrom": map[string]interface{}{
													"secretKeyRef": map[string]interface{}{"name": "mysecret", "key": "password"},
												},
											},
										},
									},
								},
								"initContainers": []interface{}{
									map[string]interface{}{
										"name": "init-container",
										"env": []interface{}{
											map[string]interface{}{
												"name":  "INIT_SECRET",
												"value": "initsecret",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"spec": map[string]interface{}{
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name": "main-container",
										"env": []interface{}{
											map[string]interface{}{
												"name":  "SECRET_KEY",
												"value": maskedValue, // Should be masked
											},
											map[string]interface{}{
												"name": "FROM_SECRET",
												"valueFrom": map[string]interface{}{ // Should NOT be masked
													"secretKeyRef": map[string]interface{}{"name": "mysecret", "key": "password"},
												},
											},
										},
									},
								},
								"initContainers": []interface{}{
									map[string]interface{}{
										"name": "init-container",
										"env": []interface{}{
											map[string]interface{}{
												"name":  "INIT_SECRET",
												"value": maskedValue, // Should be masked
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "CronJob with env vars",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "batch/v1",
					"kind":       "CronJob",
					"spec": map[string]interface{}{
						"jobTemplate": map[string]interface{}{
							"spec": map[string]interface{}{
								"template": map[string]interface{}{
									"spec": map[string]interface{}{
										"containers": []interface{}{
											map[string]interface{}{
												"name": "cron-container",
												"env": []interface{}{
													map[string]interface{}{
														"name":  "CRON_SECRET",
														"value": "cronsecretvalue",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "batch/v1",
					"kind":       "CronJob",
					"spec": map[string]interface{}{
						"jobTemplate": map[string]interface{}{
							"spec": map[string]interface{}{
								"template": map[string]interface{}{
									"spec": map[string]interface{}{
										"containers": []interface{}{
											map[string]interface{}{
												"name": "cron-container",
												"env": []interface{}{
													map[string]interface{}{
														"name":  "CRON_SECRET",
														"value": maskedValue, // Should be masked
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Pod with ephemeral container",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name": "main-container",
							},
						},
						"ephemeralContainers": []interface{}{
							map[string]interface{}{
								"name": "debugger",
								"env": []interface{}{
									map[string]interface{}{
										"name":  "DEBUG_KEY",
										"value": "debug-value",
									},
								},
							},
						},
					},
				},
			},
			expected: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name": "main-container",
							},
						},
						"ephemeralContainers": []interface{}{
							map[string]interface{}{
								"name": "debugger",
								"env": []interface{}{
									map[string]interface{}{
										"name":  "DEBUG_KEY",
										"value": maskedValue,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Resource without pod spec (Service)",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Service",
					"spec": map[string]interface{}{
						"selector": map[string]interface{}{
							"app": "MyApp",
						},
					},
				},
			},
			expected: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Service",
					"spec": map[string]interface{}{
						"selector": map[string]interface{}{
							"app": "MyApp",
						},
					},
				},
			},
		},
		{
			name: "Deployment with malformed env entry",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"spec": map[string]interface{}{
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name": "main-container",
										"env": []interface{}{
											"this-is-not-a-map", // Malformed entry
											map[string]interface{}{
												"name":  "GOOD_KEY",
												"value": "good-value",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"spec": map[string]interface{}{
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name": "main-container",
										"env": []interface{}{
											"this-is-not-a-map", // Should be unchanged
											map[string]interface{}{
												"name":  "GOOD_KEY",
												"value": maskedValue, // Should be masked
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Pod with no env vars",
			input: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name": "no-env-container",
							},
						},
					},
				},
			},
			expected: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Pod",
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name": "no-env-container",
							},
						},
					},
				},
			},
		},

		{
			name:     "Nil input",
			input:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := MaskEnvironmentVariables(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, tt.input)
		})
	}
}
