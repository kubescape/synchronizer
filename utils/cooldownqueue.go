package utils

import (
	"strings"
	"sync"
	"time"

	"istio.io/pkg/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	defaultExpiration = 5 * time.Second
	evictionInterval  = 1 * time.Second
)

// CooldownQueue is a queue that lets clients put events into it with a cooldown
//
// When a client puts an event into a queue, it waits for a cooldown period before
// the event is forwarded to the consumer. If and event for the same key is put into the queue
// again before the cooldown period is over, the event is overridden and the cooldown period is reset.
type CooldownQueue struct {
	state      *cooldownQueueState
	stopOnce   sync.Once
	seenEvents cache.ExpiringCache
	// inner channel for producing events
	innerChan chan watch.Event
	// public channel for reading events
	ResultChan <-chan watch.Event
}

// cooldownQueueState is shared with the eviction callback. It must not reference
// the queue or cache: the cache wrapper needs to become unreachable for Istio's
// finalizer to stop the eviction goroutine.
type cooldownQueueState struct {
	mu   sync.RWMutex
	done chan struct{}
}

func (s *cooldownQueueState) closed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// NewCooldownQueue returns a new Cooldown Queue
func NewCooldownQueue() *CooldownQueue {
	events := make(chan watch.Event)
	state := &cooldownQueueState{done: make(chan struct{})}
	q := &CooldownQueue{innerChan: events, ResultChan: events, state: state}
	callback := func(key, value any) {
		state.mu.RLock()
		defer state.mu.RUnlock()
		if state.closed() {
			return
		}
		select {
		case <-state.done:
		case events <- value.(watch.Event):
		}
	}
	q.seenEvents = cache.NewTTLWithCallback(defaultExpiration, evictionInterval, callback)
	return q
}

// makeEventKey creates a unique key for an event from a watcher
func makeEventKey(e watch.Event) string {
	gvk := e.Object.GetObjectKind().GroupVersionKind()
	meta := e.Object.(metav1.Object)
	return strings.Join([]string{gvk.Group, gvk.Version, gvk.Kind, meta.GetNamespace(), meta.GetName()}, "/")
}

func (q *CooldownQueue) Closed() bool {
	return q.state.closed()
}

// Enqueue enqueues an event in the Cooldown Queue
func (q *CooldownQueue) Enqueue(e watch.Event) {
	q.state.mu.RLock()
	defer q.state.mu.RUnlock()
	if q.Closed() {
		return
	}
	eventKey := makeEventKey(e)
	q.seenEvents.Set(eventKey, e)
}

func (q *CooldownQueue) Stop() {
	q.stopOnce.Do(func() {
		// Unblock an eviction waiting for a consumer before waiting for its read lock.
		close(q.state.done)
		q.state.mu.Lock()
		defer q.state.mu.Unlock()
		close(q.innerChan)
	})
}
