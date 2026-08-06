package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A signal arriving after initialization stops watching for one, but before the serving
// shutdown path reads it, must still be delivered: left unhandled, SIGTERM's default
// disposition kills the process and the deferred message queue shutdown never runs, so
// buffered producer records are dropped instead of flushed.
func TestNotifyShutdownSignals_CatchesSignalDuringInitHandoff(t *testing.T) {
	signals, stopSignals := notifyShutdownSignals()
	defer stopSignals()

	// a second receiver so a regression fails this assertion instead of killing the test
	// binary outright on the default disposition
	canary := make(chan os.Signal, 1)
	signal.Notify(canary, syscall.SIGTERM)
	defer signal.Stop(canary)

	// the hand-off itself: initialization registers its own receiver and then drops it,
	// exactly as main does once the message queue is up
	initCtx, stopInit := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	require.NoError(t, initCtx.Err())
	stopInit()

	self, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)
	require.NoError(t, self.Signal(syscall.SIGTERM))

	select {
	case sig := <-signals:
		assert.Equal(t, syscall.SIGTERM, sig)
	case <-time.After(5 * time.Second):
		t.Fatal("SIGTERM sent during the initialization hand-off never reached the shutdown receiver")
	}
}
