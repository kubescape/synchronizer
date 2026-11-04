package backend

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/telemetry"
)

type bufferedMessage struct {
	message  messaging.IncomingMessage
	handler  *messaging.MessageHandler
	adapter  adapters.Adapter
	client   string
	expires  time.Time
	bytes    int
	global   *list.Element
	local    *list.Element
	inFlight bool
}

type clientRouteLock struct {
	sync.Mutex
	refs int
}

// The global mutex protects only bookkeeping, never websocket I/O. Routing locks
// live only while a caller owns or waits for them, so disconnected client names
// cannot accumulate an unbounded second cache.
type replayBuffer struct {
	mu                   sync.Mutex
	ordered              list.List
	clients              map[string]*list.List
	routes               map[string]*clientRouteLock
	bytes                int
	ttl                  time.Duration
	maxMessages          int
	maxMessagesPerClient int
	maxBytes             int
	closed               bool
}

func newReplayBuffer(ctx context.Context, cfg *config.ReplayConfig) *replayBuffer {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	r := &replayBuffer{
		clients: make(map[string]*list.List), routes: make(map[string]*clientRouteLock),
		ttl: 5 * time.Minute, maxMessages: 10000, maxMessagesPerClient: 100, maxBytes: 64 << 20,
	}
	if cfg.TTLSeconds > 0 && int64(cfg.TTLSeconds) <= int64((1<<63-1)/time.Second) {
		r.ttl = time.Duration(cfg.TTLSeconds) * time.Second
	}
	if cfg.MaxMessages > 0 {
		r.maxMessages = cfg.MaxMessages
	}
	if cfg.MaxMessagesPerClient > 0 {
		r.maxMessagesPerClient = cfg.MaxMessagesPerClient
	}
	if cfg.MaxBytes > 0 {
		r.maxBytes = cfg.MaxBytes
	}
	go func() {
		ticker := time.NewTicker(min(r.ttl, time.Second))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				r.mu.Lock()
				r.closed = true
				for elem := r.ordered.Front(); elem != nil; {
					next := elem.Next()
					entry := elem.Value.(*bufferedMessage)
					if !entry.inFlight {
						r.remove(entry)
					}
					elem = next
				}
				r.mu.Unlock()
				return
			case <-ticker.C:
				r.mu.Lock()
				r.expire()
				r.mu.Unlock()
			}
		}
	}()
	return r
}

func (r *replayBuffer) lockClient(client string) func() {
	r.mu.Lock()
	l := r.routes[client]
	if l == nil {
		l = &clientRouteLock{}
		r.routes[client] = l
	}
	l.refs++
	r.mu.Unlock()
	l.Lock()
	return func() {
		l.Unlock()
		r.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(r.routes, client)
		}
		r.mu.Unlock()
	}
}

func (r *replayBuffer) remove(entry *bufferedMessage) {
	if entry.global == nil {
		return // expiry or capacity eviction may win while a callback is in flight
	}
	r.ordered.Remove(entry.global)
	queue := r.clients[entry.client]
	queue.Remove(entry.local)
	if queue.Len() == 0 {
		delete(r.clients, entry.client)
	}
	r.bytes -= entry.bytes
	entry.global = nil
}

func (r *replayBuffer) expire() {
	now := time.Now()
	for front := r.ordered.Front(); front != nil; {
		entry := front.Value.(*bufferedMessage)
		if now.Before(entry.expires) {
			break
		}
		next := front.Next()
		if !entry.inFlight {
			r.remove(entry)
			telemetry.Default().Drop(context.Background(), "replay_expired")
		}
		front = next
	}
}

func (r *replayBuffer) add(client string, msg messaging.IncomingMessage, handler *messaging.MessageHandler, adapter adapters.Adapter) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	r.expire()
	size := len(msg.Payload) + len(msg.ID)
	for k, v := range msg.Properties {
		size += len(k) + len(v)
	}
	if size > r.maxBytes {
		telemetry.Default().Drop(context.Background(), "replay_oversized")
		return fmt.Errorf("backend message exceeds replay maxBytes: %d > %d", size, r.maxBytes)
	}
	if queue := r.clients[client]; queue != nil && queue.Len() >= r.maxMessagesPerClient {
		r.remove(queue.Front().Value.(*bufferedMessage))
		telemetry.Default().Drop(context.Background(), "replay_capacity")
	}
	for r.ordered.Len() >= r.maxMessages || size > r.maxBytes-r.bytes {
		candidate := r.ordered.Front()
		for candidate != nil && candidate.Value.(*bufferedMessage).inFlight {
			candidate = candidate.Next()
		}
		if candidate == nil {
			telemetry.Default().Drop(context.Background(), "replay_capacity")
			return fmt.Errorf("replay capacity is occupied by messages in flight")
		}
		r.remove(candidate.Value.(*bufferedMessage))
		telemetry.Default().Drop(context.Background(), "replay_capacity")
	}
	// The broker owns its payload and properties. Retain only bounded copies.
	copyMsg := messaging.IncomingMessage{ID: strings.Clone(msg.ID), Payload: append([]byte(nil), msg.Payload...), Properties: make(map[string]string, len(msg.Properties))}
	for k, v := range msg.Properties {
		copyMsg.Properties[strings.Clone(k)] = strings.Clone(v)
	}
	entry := &bufferedMessage{message: copyMsg, handler: handler, adapter: adapter, client: client, expires: time.Now().Add(r.ttl), bytes: size}
	queue := r.clients[client]
	if queue == nil {
		queue = list.New()
		r.clients[client] = queue
	}
	entry.local = queue.PushBack(entry)
	entry.global = r.ordered.PushBack(entry)
	r.bytes += size
	return nil
}

func (b *Adapter) replayPending(ctx context.Context, client string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.mainContext.Err(); err != nil {
			return err
		}
		b.replay.mu.Lock()
		b.replay.expire()
		queue := b.replay.clients[client]
		if queue == nil {
			b.replay.mu.Unlock()
			return nil
		}
		entry := queue.Front().Value.(*bufferedMessage)
		entry.inFlight = true
		b.replay.mu.Unlock()
		err := entry.handler.Deliver(ctx, entry.adapter, entry.message)
		b.replay.mu.Lock()
		entry.inFlight = false
		if err == nil || b.replay.closed {
			b.replay.remove(entry)
		}
		b.replay.mu.Unlock()
		if err != nil {
			// Keep the failed entry and its successors until a later attempt or expiry.
			return fmt.Errorf("replay backend message: %w", err)
		}
	}
}

// HandleBackendMessage and Start share the same per-client lock: a message is
// either buffered before connection publication or delivered after its backlog.
func (b *Adapter) HandleBackendMessage(ctx context.Context, adapter adapters.Adapter, msg messaging.IncomingMessage, handler *messaging.MessageHandler) error {
	if b.replay == nil {
		return handler.Deliver(ctx, adapter, msg)
	}
	id := domain.ClientIdentifier{Account: msg.Properties[messaging.MsgPropAccount], Cluster: msg.Properties[messaging.MsgPropCluster]}
	unlock := b.replay.lockClient(id.String())
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.mainContext.Err(); err != nil {
		return err
	}
	buffer := func() error {
		if len(msg.Payload) > b.replay.maxBytes {
			telemetry.Default().Drop(ctx, "replay_oversized")
			return fmt.Errorf("backend message exceeds replay maxBytes: %d > %d", len(msg.Payload), b.replay.maxBytes)
		}
		if err := validateReplayMessage(ctx, msg); err != nil {
			return err
		}
		return b.replay.add(id.String(), msg, handler, adapter)
	}
	if adapter.IsRelated(ctx, id) {
		if err := b.replayPending(ctx, id.String()); err != nil {
			if bufferErr := buffer(); bufferErr != nil {
				return fmt.Errorf("%w; buffering live message: %v", err, bufferErr)
			}
			return err
		}
		if err := handler.Deliver(ctx, adapter, msg); err != nil {
			if bufferErr := buffer(); bufferErr != nil {
				return fmt.Errorf("%w; buffering failed delivery: %v", err, bufferErr)
			}
			return err
		}
		return nil
	}
	if id.Account != "" && id.Cluster != "" {
		switch msg.Properties[messaging.MsgPropEvent] {
		case messaging.MsgPropEventValuePutObjectMessage, messaging.MsgPropEventValuePatchObjectMessage,
			messaging.MsgPropEventValueDeleteObjectMessage, messaging.MsgPropEventValueGetObjectMessage,
			messaging.MsgPropEventValueVerifyObjectMessage, messaging.MsgPropEventValueReconciliationRequestMessage:
			return buffer()
		}
	}
	return handler.Deliver(ctx, adapter, msg)
}

// OrderedBackendMessages asks the queue reader to preserve receive order rather
// than dispatching concurrently through its configured worker pool.
func (b *Adapter) OrderedBackendMessages() bool { return b.replay != nil }

// Reject malformed records before they can become a backlog head that prevents
// every later valid command from replaying on connect.
func validateReplayMessage(ctx context.Context, msg messaging.IncomingMessage) error {
	var payload any
	switch msg.Properties[messaging.MsgPropEvent] {
	case messaging.MsgPropEventValuePutObjectMessage:
		payload = &messaging.PutObjectMessage{}
	case messaging.MsgPropEventValuePatchObjectMessage:
		payload = &messaging.PatchObjectMessage{}
	case messaging.MsgPropEventValueDeleteObjectMessage:
		payload = &messaging.DeleteObjectMessage{}
	case messaging.MsgPropEventValueGetObjectMessage:
		payload = &messaging.GetObjectMessage{}
	case messaging.MsgPropEventValueVerifyObjectMessage:
		payload = &messaging.VerifyObjectMessage{}
	case messaging.MsgPropEventValueReconciliationRequestMessage:
		payload = &messaging.ReconciliationRequestMessage{}
	default:
		return fmt.Errorf("unknown replay message type")
	}
	if err := json.Unmarshal(msg.Payload, payload); err != nil {
		return fmt.Errorf("invalid replay message: %w", err)
	}
	if batch, ok := payload.(*messaging.ReconciliationRequestMessage); ok {
		for kind := range batch.KindToObjects {
			if domain.KindFromString(ctx, kind) == nil {
				return fmt.Errorf("unknown kind %q in replay batch", kind)
			}
		}
		return nil
	}
	var header struct{ Kind string }
	if err := json.Unmarshal(msg.Payload, &header); err != nil {
		return err
	}
	if domain.KindFromString(ctx, header.Kind) == nil {
		return fmt.Errorf("unknown kind %q in replay message", header.Kind)
	}
	return nil
}
