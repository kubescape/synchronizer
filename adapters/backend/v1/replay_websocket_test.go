package backend

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws/wsutil"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
	"github.com/stretchr/testify/require"
)

func TestReplayReadsClientWhileDrainingBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	handler := &messaging.MessageHandler{}
	for _, name := range []string{"one", "two", "three", "four"} {
		require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, name)))
	}
	connCtx := connectionContext("connection", time.Now(), nil)
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	server, err := core.NewSynchronizerServer(connCtx, []adapters.Adapter{adapter}, serverConn)
	require.NoError(t, err)
	defer server.Stop(connCtx)
	go server.Start(connCtx)
	// An unbuffered transport exposes peers that need to send a response before
	// they can read the next replayed message.
	require.NoError(t, clientConn.SetDeadline(time.Now().Add(2*time.Second)))
	for range 4 {
		_, err := wsutil.ReadServerBinary(clientConn)
		require.NoError(t, err)
		require.NoError(t, wsutil.WriteClientBinary(clientConn, []byte(`{"event":"ping"}`)))
	}
}

type commandConsumer struct {
	*adapters.MockAdapter
	applied chan string
	count   atomic.Int32
}

type replayWriteSignal struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *replayWriteSignal) Write(data []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(data)
}

func TestReplayStopUnblocksStalledWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := NewBackendAdapter(ctx, silentProducer{}, replayConfig(t, `{"enabled":true}`))
	handler := &messaging.MessageHandler{}
	for _, name := range []string{"one", "two", "three"} {
		require.NoError(t, handler.Handle(ctx, adapter, replayMessage(t, name)))
	}
	connCtx := connectionContext("connection", time.Now(), nil)
	local, peer := net.Pipe()
	defer peer.Close()
	conn := &replayWriteSignal{Conn: local, started: make(chan struct{})}
	server, err := core.NewSynchronizerServer(connCtx, []adapters.Adapter{adapter}, conn)
	require.NoError(t, err)
	defer server.Stop(connCtx)
	started := make(chan error, 1)
	go func() { started <- server.Start(connCtx) }()
	select {
	case <-conn.started:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not start")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Stop(connCtx) }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop deadlocked with replay")
	}
	select {
	case err := <-started:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start stayed blocked in replay")
	}
}

func (a *commandConsumer) PutObject(_ context.Context, id domain.KindName, _ string, _ []byte) error {
	// Model a controller consuming and deleting its command immediately. A second
	// Put would execute it again even though Kubernetes Create/Update is idempotent.
	a.count.Add(1)
	a.applied <- id.Name
	return nil
}

func TestReplayWebSocketReconnect(t *testing.T) {
	for _, deliveredOnOld := range []bool{false, true} {
		name := "dead old connection"
		if deliveredOnOld {
			name = "old connection already applied command"
		}
		t.Run(name, func(t *testing.T) {
			mainCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := replayConfig(t, `{"enabled":true}`)
			oldPod := NewBackendAdapter(mainCtx, silentProducer{}, cfg)
			newPod := NewBackendAdapter(mainCtx, silentProducer{}, cfg)
			oldCtx := connectionContext("old", time.Now(), nil)
			newCtx := connectionContext("new", time.Now().Add(time.Second), nil)
			consumer := &commandConsumer{MockAdapter: adapters.NewMockAdapter(true), applied: make(chan string, 10)}
			clientConn, oldServerConn := net.Pipe()
			oldServer, err := core.NewSynchronizerServer(oldCtx, []adapters.Adapter{oldPod}, oldServerConn)
			require.NoError(t, err)
			defer oldServer.Stop(oldCtx)
			serverDone := make(chan error, 1)
			go func() { serverDone <- oldServer.Start(oldCtx) }()
			require.Eventually(t, func() bool { return oldPod.IsRelated(oldCtx, utils.ClientIdentifierFromContext(oldCtx)) }, 5*time.Second, time.Millisecond)
			newServers := make(chan *core.Synchronizer, 1)
			newServerDone := make(chan error, 1)
			client, err := core.NewSynchronizerClient(oldCtx, []adapters.Adapter{consumer}, clientConn, func() (net.Conn, error) {
				local, remote := net.Pipe()
				server, err := core.NewSynchronizerServer(newCtx, []adapters.Adapter{newPod}, remote)
				if err != nil {
					local.Close()
					remote.Close()
					return nil, err
				}
				newServers <- server
				go func() { newServerDone <- server.Start(newCtx) }()
				return local, nil
			})
			require.NoError(t, err)
			defer client.Stop(oldCtx)
			clientDone := make(chan error, 1)
			go func() { clientDone <- client.Start(oldCtx) }()
			waitApplied := func(want string) {
				t.Helper()
				select {
				case got := <-consumer.applied:
					require.Equal(t, want, got)
				case <-time.After(5 * time.Second):
					t.Fatalf("command %s was not applied", want)
				}
			}
			handler := &messaging.MessageHandler{}
			msg := replayMessage(t, "command")
			if deliveredOnOld {
				require.NoError(t, handler.Handle(mainCtx, oldPod, msg))
				waitApplied("command")
			} else {
				// Simulate TCP accepting an old-pod write while the vanished peer
				// receives nothing. The actual new-pod delivery uses WebSockets.
				oldPod.RegisterCallbacks(oldCtx, domain.Callbacks{PutObject: func(context.Context, domain.KindName, string, []byte) error { return nil }})
				require.NoError(t, handler.Handle(mainCtx, oldPod, msg))
			}
			require.NoError(t, handler.Handle(mainCtx, newPod, msg))
			require.True(t, oldPod.IsRelated(oldCtx, utils.ClientIdentifierFromContext(oldCtx)))
			require.NoError(t, oldServerConn.Close())
			// A client write takes the production reconnect path and installs the
			// new socket in the existing client, retaining its applied-message cache.
			require.NoError(t, client.PutObjectCallback(oldCtx, domain.KindName{Kind: domain.KindFromString(oldCtx, "/v1/configmaps"), Name: "upstream"}, "", []byte(`{}`)))
			var newServer *core.Synchronizer
			select {
			case newServer = <-newServers:
			case <-time.After(5 * time.Second):
				t.Fatal("client did not reconnect")
			}
			defer newServer.Stop(newCtx)
			if !deliveredOnOld {
				waitApplied("command")
			}
			require.NoError(t, handler.Handle(mainCtx, newPod, replayMessage(t, "barrier")))
			waitApplied("barrier")
			require.Equal(t, int32(2), consumer.count.Load(), "command and barrier must each apply once across both sockets")
			require.NoError(t, client.Stop(oldCtx))
			require.NoError(t, oldServer.Stop(oldCtx))
			require.NoError(t, newServer.Stop(newCtx))
			for _, done := range []chan error{clientDone, serverDone, newServerDone} {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("synchronizer did not stop")
				}
			}
		})
	}
}
