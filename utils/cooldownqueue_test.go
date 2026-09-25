package utils

import (
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// The cache wrapper's finalizer must be able to stop its eviction goroutine.
// A callback that retains the queue also retains that wrapper, preventing GC.
func TestCooldownQueue_DiscardReleasesEvicter(t *testing.T) {
	evicters := func() map[string]bool {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			t.Fatal("goroutine stack buffer too small")
		}
		ids := make(map[string]bool)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(stack, "istio.io/pkg/cache.(*ttlCache).evicter(") {
				ids[strings.Fields(stack)[1]] = true
			}
		}
		return ids
	}
	for _, stopped := range []bool{false, true} {
		name := "discarded"
		if stopped {
			name = "stopped"
		}
		t.Run(name, func(t *testing.T) {
			before := evicters()
			q := NewCooldownQueue()
			if stopped {
				q.Enqueue(podAdded)
				q.Stop()
			}
			var created map[string]bool
			assert.Eventually(t, func() bool {
				created = evicters()
				for id := range before {
					delete(created, id)
				}
				return len(created) > 0
			}, time.Second, time.Millisecond)
			runtime.KeepAlive(q)
			q = nil
			assert.Eventually(t, func() bool {
				runtime.GC()
				for id := range evicters() {
					if created[id] {
						return false
					}
				}
				return true
			}, 5*time.Second, 10*time.Millisecond, "discarded queue retained its cache eviction goroutine")
		})
	}
}

var (
	configmap       = unstructured.Unstructured{Object: map[string]any{"kind": "ConfigMap", "metadata": map[string]any{"uid": "748ad4a8-e5ff-44da-ba94-309992c97820"}}}
	deployment      = unstructured.Unstructured{Object: map[string]any{"kind": "Deployment", "metadata": map[string]any{"uid": "6b1a0c50-277f-4aa1-a4f9-9fc278ce4fe2"}}}
	pod             = unstructured.Unstructured{Object: map[string]any{"kind": "Pod", "metadata": map[string]any{"uid": "aa5e3e8f-2da5-4c38-93c0-210d3280d10f"}}}
	deploymentAdded = watch.Event{Type: watch.Added, Object: &deployment}
	podAdded        = watch.Event{Type: watch.Added, Object: &pod}
	podModified     = watch.Event{Type: watch.Modified, Object: &pod}
)

func TestCooldownQueue_Enqueue(t *testing.T) {
	tests := []struct {
		name      string
		inEvents  []watch.Event
		outEvents []watch.Event
	}{
		{
			name:      "add pod",
			inEvents:  []watch.Event{deploymentAdded, podAdded, podModified, podModified, podModified},
			outEvents: []watch.Event{deploymentAdded, podModified},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewCooldownQueue()
			go func() {
				time.Sleep(10 * time.Second)
				q.Stop()
			}()
			for _, e := range tt.inEvents {
				time.Sleep(50 * time.Millisecond) // need to sleep to preserve order since the insertion is async
				q.Enqueue(e)
			}
			outEvents := []watch.Event{}
			for e := range q.ResultChan {
				outEvents = append(outEvents, e)
			}
			// sort outEvents to make the comparison easier
			sort.Slice(outEvents, func(i, j int) bool {
				uidI := outEvents[i].Object.(*unstructured.Unstructured).GetUID()
				uidJ := outEvents[j].Object.(*unstructured.Unstructured).GetUID()
				return uidI < uidJ
			})
			assert.Equal(t, tt.outEvents, outEvents)
		})
	}
}

// Stopping the queue while an enqueued event's cooldown is still pending
// used to panic: the TTL cache's eviction goroutine outlives Stop() and
// tries to send the evicted event on the now-closed channel.
func TestCooldownQueue_StopWhileCooldownPending(t *testing.T) {
	q := NewCooldownQueue()
	go func() {
		for range q.ResultChan { //nolint:revive // drain, nothing to assert on
		}
	}()
	q.Enqueue(podAdded)
	q.Stop()
	time.Sleep(defaultExpiration + evictionInterval + 2*time.Second)
}

// key is only based on the UID of the object
func Test_makeEventKey(t *testing.T) {
	tests := []struct {
		name string
		e    watch.Event
		want string
	}{
		{
			name: "add pod",
			e: watch.Event{
				Type:   watch.Added,
				Object: &pod,
			},
			want: "//Pod//",
		},
		{
			name: "delete deployment",
			e: watch.Event{
				Type:   watch.Deleted,
				Object: &deployment,
			},
			want: "//Deployment//",
		},
		{
			name: "modify configmap",
			e: watch.Event{
				Type:   watch.Modified,
				Object: &configmap,
			},
			want: "//ConfigMap//",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := makeEventKey(tt.e)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCooldownQueue_ConcurrentStopAndEnqueue(t *testing.T) {
	q := NewCooldownQueue()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { q.Enqueue(podAdded); q.Stop(); assert.True(t, q.Closed()) })
	}
	wg.Wait()
	_, open := <-q.ResultChan
	assert.False(t, open)
}

func TestCooldownQueue_StopUnblocksEviction(t *testing.T) {
	q := NewCooldownQueue()
	q.Enqueue(podAdded)
	// Leave ResultChan unread until an eviction is blocked trying to send.
	time.Sleep(defaultExpiration + 2*evictionInterval)
	stopped := make(chan struct{})
	go func() { q.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked on an eviction without a consumer")
	}
	_, open := <-q.ResultChan
	assert.False(t, open)
}
