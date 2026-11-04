package backend

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/utils"
)

type pendingReconciliation struct {
	cancel context.CancelFunc
}

// Both helpers are called with connMapMutex held, so cancellation and dispatch
// are serialized with registration, replacement and removal of a connection.
func (a *Adapter) cancelReconciliationAfterConnect(connection string) {
	if pending := a.pendingReconciliations[connection]; pending != nil {
		pending.cancel()
		delete(a.pendingReconciliations, connection)
	}
}

func (a *Adapter) scheduleReconciliationAfterConnect(ctx context.Context, client *Client) {
	id := utils.ClientIdentifierFromContext(ctx)
	connection := id.ConnectionString()
	a.cancelReconciliationAfterConnect(connection)
	seconds := a.reconcileAfterConnectSeconds
	if seconds <= 0 || !utils.IsBatchMessageSupported(id.SyncVersion) {
		return
	}
	// Leave room for the jitter when converting seconds to a duration.
	const maxSeconds = int64((1<<63 - 1) / time.Second / 6 * 5)
	if int64(seconds) > maxSeconds {
		logger.L().Warning("reconcileAfterConnectSeconds is too large; connection reconciliation is disabled")
		return
	}
	delay := time.Duration(seconds) * time.Second
	delay += time.Duration(rand.Int64N(int64(delay/5) + 1))
	waitCtx, cancel := context.WithCancel(ctx)
	pending := &pendingReconciliation{cancel: cancel}
	if a.pendingReconciliations == nil {
		a.pendingReconciliations = make(map[string]*pendingReconciliation)
	}
	a.pendingReconciliations[connection] = pending
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		defer cancel()
		select {
		case <-waitCtx.Done():
		case <-a.mainContext.Done():
		case <-timer.C:
		}

		a.connMapMutex.Lock()
		defer a.connMapMutex.Unlock()
		if a.pendingReconciliations[connection] != pending {
			return
		}
		delete(a.pendingReconciliations, connection)
		current, ok := a.connectionMap[id.String()]
		if waitCtx.Err() != nil || a.mainContext.Err() != nil || !ok || current.ConnectionString() != connection {
			return
		}
		// Keep the ownership check and enqueue under the same lock, as the periodic
		// task does. Use the connection context: cancelling the timer's context on
		// return must not cancel delivery by an asynchronous message producer.
		if err := client.SendReconciliationRequestMessage(ctx); err != nil {
			logger.L().Error("failed to send reconciliation request after connection",
				helpers.String("clientId", id.String()), helpers.Error(err))
		}
	}()
}
