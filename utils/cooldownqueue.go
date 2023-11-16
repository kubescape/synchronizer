package utils

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	// Default size for the cooldown queue
	DefaultQueueSize = 512
	// Default TTL for events put in the queue
	DefaultTTL = 5 * time.Second
)

// CooldownQueue is a queue that lets clients put events into it with a cooldown
//
// When a client puts an event into a queue, it forwards the event to its
// output channel and starts a cooldown for this event. If a client attempts to
// put the same event into the queue while the cooldown is running, the queue
// will silently drop the event. When the cooldown resets and a client puts the
// same event into the queue, it will be forwarded to the output channel
type CooldownQueue struct {
	seenEvents *lru.LRU[string, bool]
	// inner channel for producing events
	innerChan chan watch.Event
	// public channel for reading events
	ResultChan <-chan watch.Event
}

// NewCooldownQueue returns a new Cooldown Queue
func NewCooldownQueue(size int, cooldown time.Duration) *CooldownQueue {
	cache := lru.NewLRU[string, bool](size, nil, cooldown)
	events := make(chan watch.Event)
	return &CooldownQueue{
		seenEvents: cache,
		innerChan:  events,
		ResultChan: events,
	}
}

// makeEventKey creates a unique key for an event from a watcher
func makeEventKey(e watch.Event) string {
	object, ok := e.Object.(*unstructured.Unstructured)
	if !ok {
		return ""
	}
	eventKey := string(e.Type) + "-" + string(object.GetUID())
	return eventKey
}

// Enqueue enqueues an event in the Cooldown Queue
func (q *CooldownQueue) Enqueue(e watch.Event) {
	eventKey := makeEventKey(e)
	_, exists := q.seenEvents.Get(eventKey)
	if exists {
		return
	}
	go func() {
		q.innerChan <- e
	}()
	q.seenEvents.Add(eventKey, true)
}

func (q *CooldownQueue) Stop() {
	close(q.innerChan)
}
