package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errConsumerClosed = errors.New("ConsumerClosed: consumer closed")

type fakePulsarMessage struct {
	pulsar.Message
	key string
}

func (m fakePulsarMessage) Properties() map[string]string { return nil }
func (m fakePulsarMessage) Key() string                   { return m.key }
func (m fakePulsarMessage) ID() pulsar.MessageID          { return pulsar.EarliestMessageID() }

type readResult struct {
	msg pulsar.Message
	err error
}

// fakePulsarReader returns the scripted results in order and then blocks until
// the context is cancelled, like a reader on an idle topic.
type fakePulsarReader struct {
	pulsar.Reader
	mu      sync.Mutex
	results []readResult
	closed  int
}

func (r *fakePulsarReader) Next(ctx context.Context) (pulsar.Message, error) {
	r.mu.Lock()
	if len(r.results) > 0 {
		res := r.results[0]
		r.results = r.results[1:]
		r.mu.Unlock()
		return res.msg, res.err
	}
	r.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *fakePulsarReader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed++
}

func (r *fakePulsarReader) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

type fatalRecorder struct {
	mu    sync.Mutex
	calls []error
}

func (f *fatalRecorder) fatal(_ string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, err)
}

func (f *fatalRecorder) errors() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.calls...)
}

// readerFactory hands out the scripted readers/errors in order and counts calls.
type readerFactory struct {
	mu      sync.Mutex
	results []func() (pulsar.Reader, error)
	calls   int
}

func (f *readerFactory) newReader() (pulsar.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.results) == 0 {
		return nil, errors.New("unexpected reader creation")
	}
	next := f.results[0]
	f.results = f.results[1:]
	return next()
}

func (f *readerFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func returnsReader(r pulsar.Reader) func() (pulsar.Reader, error) {
	return func() (pulsar.Reader, error) { return r, nil }
}

func returnsError(err error) func() (pulsar.Reader, error) {
	return func() (pulsar.Reader, error) { return nil, err }
}

func testRecoveryPolicy() readerRecoveryPolicy {
	return readerRecoveryPolicy{
		maxAttempts:    3,
		initialBackoff: time.Millisecond,
		maxBackoff:     4 * time.Millisecond,
		healthyAfter:   time.Hour,
	}
}

func newTestPulsarMessageReader(initial pulsar.Reader, factory *readerFactory, fatal *fatalRecorder) *PulsarMessageReader {
	return &PulsarMessageReader{
		name:           "test-reader",
		reader:         initial,
		newReader:      factory.newReader,
		recovery:       testRecoveryPolicy(),
		fatal:          fatal.fatal,
		done:           make(chan bool),
		messageChannel: make(chan pulsar.Message, 10),
	}
}

// runReaderLoop runs readerLoop in the background and returns a channel that is
// closed when the loop returns.
func runReaderLoop(ctx context.Context, c *PulsarMessageReader) <-chan struct{} {
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		c.readerLoop(ctx)
	}()
	return exited
}

func requireMessage(t *testing.T, c *PulsarMessageReader, wantKey string) {
	t.Helper()
	select {
	case msg := <-c.messageChannel:
		assert.Equal(t, wantKey, msg.Key())
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message %q", wantKey)
	}
}

func requireExited(t *testing.T, exited <-chan struct{}) {
	t.Helper()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the reader loop to exit")
	}
}

func requireStillRunning(t *testing.T, exited <-chan struct{}) {
	t.Helper()
	select {
	case <-exited:
		t.Fatal("reader loop exited while it should keep consuming")
	case <-time.After(50 * time.Millisecond):
	}
}

// A reader that fails outside of a shutdown must be replaced: the loop used to
// return silently on a closed consumer, leaving the process up but deaf.
func TestReaderLoop_RecreatesReaderOnLoss(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "consumer closed", err: errConsumerClosed},
		{name: "other read error", err: errors.New("connection reset")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lost := &fakePulsarReader{results: []readResult{
				{msg: fakePulsarMessage{key: "before"}},
				{err: tt.err},
			}}
			replacement := &fakePulsarReader{results: []readResult{
				{msg: fakePulsarMessage{key: "after"}},
			}}
			factory := &readerFactory{results: []func() (pulsar.Reader, error){returnsReader(replacement)}}
			fatal := &fatalRecorder{}
			c := newTestPulsarMessageReader(lost, factory, fatal)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exited := runReaderLoop(ctx, c)

			requireMessage(t, c, "before")
			requireMessage(t, c, "after")
			requireStillRunning(t, exited)

			assert.Equal(t, 1, factory.callCount())
			assert.Equal(t, 1, lost.closeCount(), "the lost reader must be closed")
			assert.Equal(t, 0, replacement.closeCount())
			assert.Same(t, replacement, c.currentReader())
			assert.Empty(t, fatal.errors())

			cancel()
			requireExited(t, exited)
			assert.Empty(t, fatal.errors())
		})
	}
}

func TestReaderLoop_RetriesReaderCreation(t *testing.T) {
	lost := &fakePulsarReader{results: []readResult{{err: errConsumerClosed}}}
	replacement := &fakePulsarReader{results: []readResult{{msg: fakePulsarMessage{key: "after"}}}}
	factory := &readerFactory{results: []func() (pulsar.Reader, error){
		returnsError(errors.New("broker unavailable")),
		returnsError(errors.New("broker unavailable")),
		returnsReader(replacement),
	}}
	fatal := &fatalRecorder{}
	c := newTestPulsarMessageReader(lost, factory, fatal)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := runReaderLoop(ctx, c)

	requireMessage(t, c, "after")
	requireStillRunning(t, exited)
	assert.Equal(t, 3, factory.callCount())
	assert.Empty(t, fatal.errors())
}

func TestReaderLoop_FatalWhenReaderCannotBeRecreated(t *testing.T) {
	createErr := errors.New("broker unavailable")
	lost := &fakePulsarReader{results: []readResult{{err: errConsumerClosed}}}
	factory := &readerFactory{results: []func() (pulsar.Reader, error){
		returnsError(createErr),
		returnsError(createErr),
		returnsError(createErr),
	}}
	fatal := &fatalRecorder{}
	c := newTestPulsarMessageReader(lost, factory, fatal)

	exited := runReaderLoop(context.Background(), c)
	requireExited(t, exited)

	assert.Equal(t, c.recovery.maxAttempts, factory.callCount())
	fatals := fatal.errors()
	require.Len(t, fatals, 1)
	assert.ErrorIs(t, fatals[0], createErr)
}

// A reader that is lost again right after every successful recreation must
// not be recreated forever: the attempts only reset once a message was read.
func TestReaderLoop_FatalWhenRecreatedReaderKeepsFailing(t *testing.T) {
	lost := &fakePulsarReader{results: []readResult{{err: errConsumerClosed}}}
	factory := &readerFactory{}
	for i := 0; i < 3; i++ {
		factory.results = append(factory.results,
			returnsReader(&fakePulsarReader{results: []readResult{{err: errConsumerClosed}}}))
	}
	fatal := &fatalRecorder{}
	c := newTestPulsarMessageReader(lost, factory, fatal)

	exited := runReaderLoop(context.Background(), c)
	requireExited(t, exited)

	assert.Equal(t, c.recovery.maxAttempts, factory.callCount())
	fatals := fatal.errors()
	require.Len(t, fatals, 1)
	assert.ErrorIs(t, fatals[0], errConsumerClosed)
}

// A successfully read message proves the replacement reader works, so later
// losses get the full number of attempts again.
func TestReaderLoop_AttemptsResetAfterMessage(t *testing.T) {
	lost := &fakePulsarReader{results: []readResult{{err: errConsumerClosed}}}
	factory := &readerFactory{}
	const losses = 5 // more than maxAttempts
	for i := 0; i < losses; i++ {
		results := []readResult{{msg: fakePulsarMessage{key: "msg"}}}
		if i < losses-1 {
			results = append(results, readResult{err: errConsumerClosed})
		}
		factory.results = append(factory.results, returnsReader(&fakePulsarReader{results: results}))
	}
	fatal := &fatalRecorder{}
	c := newTestPulsarMessageReader(lost, factory, fatal)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := runReaderLoop(ctx, c)

	for i := 0; i < losses; i++ {
		requireMessage(t, c, "msg")
	}
	requireStillRunning(t, exited)
	assert.Equal(t, losses, factory.callCount())
	assert.Empty(t, fatal.errors())
}

// An idle topic delivers no message to prove the replacement works; a reader
// that survived healthyAfter counts as healthy instead.
func TestReaderLoop_AttemptsResetAfterHealthyPeriod(t *testing.T) {
	lost := &fakePulsarReader{results: []readResult{{err: errConsumerClosed}}}
	factory := &readerFactory{}
	const losses = 5 // more than maxAttempts
	var last *fakePulsarReader
	for i := 0; i < losses; i++ {
		last = &fakePulsarReader{}
		if i < losses-1 {
			last.results = []readResult{{err: errConsumerClosed}}
		}
		factory.results = append(factory.results, returnsReader(last))
	}
	fatal := &fatalRecorder{}
	c := newTestPulsarMessageReader(lost, factory, fatal)
	c.recovery.healthyAfter = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := runReaderLoop(ctx, c)

	require.Eventually(t, func() bool { return factory.callCount() == losses },
		5*time.Second, time.Millisecond)
	requireStillRunning(t, exited)
	assert.Same(t, last, c.currentReader())
	assert.Empty(t, fatal.errors())
}

func TestReaderLoop_ExitsOnContextCancellation(t *testing.T) {
	t.Run("while reading", func(t *testing.T) {
		reader := &fakePulsarReader{}
		factory := &readerFactory{}
		fatal := &fatalRecorder{}
		c := newTestPulsarMessageReader(reader, factory, fatal)

		ctx, cancel := context.WithCancel(context.Background())
		exited := runReaderLoop(ctx, c)
		requireStillRunning(t, exited)
		cancel()
		requireExited(t, exited)

		assert.Equal(t, 0, factory.callCount())
		assert.Empty(t, fatal.errors())
	})

	t.Run("while waiting to recreate the reader", func(t *testing.T) {
		lost := &fakePulsarReader{results: []readResult{{err: errConsumerClosed}}}
		factory := &readerFactory{}
		fatal := &fatalRecorder{}
		c := newTestPulsarMessageReader(lost, factory, fatal)
		c.recovery.initialBackoff = time.Hour
		c.recovery.maxBackoff = time.Hour

		ctx, cancel := context.WithCancel(context.Background())
		exited := runReaderLoop(ctx, c)
		require.Eventually(t, func() bool { return lost.closeCount() == 1 },
			5*time.Second, time.Millisecond)
		cancel()
		requireExited(t, exited)

		assert.Equal(t, 0, factory.callCount())
		assert.Empty(t, fatal.errors())
	})
}

// stop() closes the reader during shutdown; a reader created concurrently by
// the recovery must not outlive it.
func TestReplaceReader_ClosesNewReaderAfterStop(t *testing.T) {
	original := &fakePulsarReader{}
	c := newTestPulsarMessageReader(original, &readerFactory{}, &fatalRecorder{})

	go func() { <-c.done }()
	c.stop()

	late := &fakePulsarReader{}
	assert.False(t, c.replaceReader(late))
	assert.Equal(t, 1, late.closeCount())
	assert.Equal(t, 1, original.closeCount())
	assert.Same(t, original, c.currentReader())
}

func TestReaderRecoveryPolicy_Backoff(t *testing.T) {
	policy := defaultReaderRecoveryPolicy()
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		30 * time.Second, 30 * time.Second,
	}
	for i, expected := range want {
		assert.Equal(t, expected, policy.backoff(i+1), "attempt %d", i+1)
	}
}
