package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadReplayAndDeduplicationConfig(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{
		"backend":{"replay":{"enabled":true,"ttlSeconds":120,"maxMessages":1000,"maxMessagesPerClient":50,"maxBytes":1048576}},
		"inCluster":{"deduplication":{"ttlSeconds":240,"maxMessages":2000}}
	}`), 0600))
	cfg, err := LoadConfig(dir)
	require.NoError(t, err)
	require.Equal(t, &ReplayConfig{Enabled: true, TTLSeconds: 120, MaxMessages: 1000, MaxMessagesPerClient: 50, MaxBytes: 1048576}, cfg.Backend.Replay)
	ttl, count := cfg.InCluster.Deduplication.DeduplicationLimits()
	require.Equal(t, 240, ttl)
	require.Equal(t, 2000, count)
	for _, tc := range []struct {
		name       string
		cfg        *DeduplicationConfig
		ttl, count int
	}{
		{"absent", nil, 600, 10000},
		{"disabled", &DeduplicationConfig{Disabled: true, TTLSeconds: 1, MaxMessages: 1}, 0, 0},
		{"negative limits", &DeduplicationConfig{TTLSeconds: -1, MaxMessages: -1}, 600, 10000},
		{"duration overflow", &DeduplicationConfig{TTLSeconds: 1 << 62}, 600, 10000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ttl, count := tc.cfg.DeduplicationLimits()
			require.Equal(t, tc.ttl, ttl)
			require.Equal(t, tc.count, count)
		})
	}
}
