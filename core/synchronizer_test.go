package core

import (
	"context"
	"encoding/json"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/adapters/backend/v1"
	"github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net"
	"os"
	"testing"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
)

var (
	kindDeployment = domain.KindName{
		Kind:      domain.KindFromString("apps/v1/Deployment"),
		Name:      "name",
		Namespace: "namespace",
	}
	kindKnownServers = domain.KindName{
		Kind:      domain.KindFromString("spdx.softwarecomposition.kubescape.io/v1beta1/KnownServers"),
		Name:      "name",
		Namespace: "namespace",
	}
	object         = []byte(`{"kind":"kind","metadata":{"name":"name","resourceVersion":"1"}}`)
	objectClientV2 = []byte(`{"kind":"kind","metadata":{"name":"client","resourceVersion":"2"}}`)
	objectServerV2 = []byte(`{"kind":"kind","metadata":{"name":"server","resourceVersion":"2"}}`)
)

type IngesterConfig struct {
	Logger                     LoggerConfig
	Postgres                   PostgresConfig
	Pulsar                     PulsarConfig
	S3Config                   S3Config
	SynchronizerIngesterConfig SynchronizerIngesterConfig
}

type LoggerConfig struct {
	Level string `json:"level,omitempty"`
}

type PostgresConfig struct {
	Enabled bool `json:"enabled,omitempty"`
}

type PulsarConfig struct {
	URL                 string   `json:"url,omitempty"`
	Tenant              string   `json:"tenant,omitempty"`
	Namespace           string   `json:"namespace,omitempty"`
	AdminUrl            string   `json:"adminUrl,omitempty"`
	Clusters            []string `json:"clusters,omitempty"`
	MaxDeliveryAttempts int      `json:"maxDeliveryAttempts,omitempty"`
}

type S3Config struct {
	AccessKey   string `json:"accessKey,omitempty"`
	SecretKey   string `json:"secretKey,omitempty"`
	Region      string `json:"region,omitempty"`
	Bucket      string `json:"bucket,omitempty"`
	Prefix      string `json:"prefix,omitempty"`
	EndPoint    string `json:"endpoint,omitempty"`
	StorageType string `json:"storageType,omitempty"`
}

type SynchronizerIngesterConfig struct {
	Enabled       bool     `json:"enabled,omitempty"`
	Topic         string   `json:"topic,omitempty"`
	Topics        []string `json:"topics,omitempty"`
	Subscription  string   `json:"subscription,omitempty"`
	WorkersNumber int      `json:"workersNumber,omitempty"`
}

type RdsCredentials struct {
	Host     string `json:"host,omitempty"`
	Port     string `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	Db       string `json:"db,omitempty"`
	Password string `json:"password,omitempty"`
	SslMode  string `json:"sslMode,omitempty"`
}

func initTest(t *testing.T) (context.Context, *adapters.MockAdapter, *adapters.MockAdapter) {
	ctx := context.WithValue(context.TODO(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: "11111111-2222-3333-4444-555555555555",
		Cluster: "cluster",
	})
	err := logger.L().SetLevel(helpers.DebugLevel.String())
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	clientAdapter := adapters.NewMockAdapter(true)
	serverAdapter := adapters.NewMockAdapter(false)
	clientConn, serverConn := net.Pipe()
	client := NewSynchronizerClient(ctx, clientAdapter, clientConn)
	server := NewSynchronizerServer(ctx, serverAdapter, serverConn)
	go func() {
		_ = client.Start(ctx)
	}()
	go func() {
		_ = server.Start(ctx)
	}()
	return ctx, clientAdapter, serverAdapter
}

func initIntegrationTest(t *testing.T) (context.Context, []testcontainers.Container, testcontainers.Network, kubernetes.Interface, pulsarconnector.Client) {
	ctx := context.WithValue(context.TODO(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: "11111111-2222-3333-4444-555555555555",
		Cluster: "cluster",
	})
	err := logger.L().SetLevel(helpers.DebugLevel.String())
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	// kwok
	kwokC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "registry.k8s.io/kwok/cluster:v0.4.0-k8s.v1.28.0",
			ExposedPorts: []string{"8080/tcp"},
			WaitingFor:   wait.ForLog("Starting to serve on"),
		},
		Started: true,
	})
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	// pulsar, postgres, ingester and s3 need to be on the same network
	backendNetworkName := "backend-network"
	backendNetwork, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name:           backendNetworkName,
			CheckDuplicate: true,
		},
	})
	// pulsar
	pulsarC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          "apachepulsar/pulsar:2.11.0",
			Cmd:            []string{"bin/pulsar", "standalone"},
			ExposedPorts:   []string{"6650/tcp", "8080/tcp"},
			NetworkAliases: map[string][]string{backendNetworkName: {"pulsar"}},
			Networks:       []string{backendNetworkName},
			WaitingFor:     wait.ForLog("messaging service is ready"),
		},
		Started: true,
	})
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	// postgres
	postgresC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16.1",
			Env: map[string]string{
				"POSTGRES_DB":       "main_db",
				"POSTGRES_USER":     "admin",
				"POSTGRES_PASSWORD": "admin",
			},
			ExposedPorts:   []string{"5432/tcp"},
			NetworkAliases: map[string][]string{backendNetworkName: {"postgres"}},
			Networks:       []string{backendNetworkName},
			WaitingFor:     wait.ForLog("database system is ready to accept connections"),
		},
		Started: true,
	})
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	// s3
	s3C, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          "adobe/s3mock:3.2.0",
			ExposedPorts:   []string{"9090/tcp"},
			NetworkAliases: map[string][]string{backendNetworkName: {"s3"}},
			Networks:       []string{backendNetworkName},
			WaitingFor:     wait.ForLog("Started S3MockApplication in"),
		},
	})
	// ingester
	ingesterConfig := &IngesterConfig{
		Logger: LoggerConfig{
			Level: "debug",
		},
		Postgres: PostgresConfig{
			Enabled: true,
		},
		Pulsar: PulsarConfig{
			URL:                 "pulsar://pulsar:6650",
			Tenant:              "armo",
			Namespace:           "event-sourcing",
			AdminUrl:            "http://pulsar:8080",
			Clusters:            []string{"standalone"},
			MaxDeliveryAttempts: 2,
		},
		S3Config: S3Config{
			Region:   "eu-west-1",
			Bucket:   "armo-ingesters-dev-stage-2h275",
			Prefix:   "development/",
			EndPoint: "http://s3:9090",
		},
		SynchronizerIngesterConfig: SynchronizerIngesterConfig{
			Enabled:       true,
			Topic:         "synchronizer-topic",
			Topics:        []string{},
			Subscription:  "synchronizer-ingester-a",
			WorkersNumber: 1,
		},
	}
	rdsCredentials := &RdsCredentials{
		Host:     "postgres",
		Port:     "5432",
		User:     "admin",
		Password: "admin",
		Db:       "main_db",
		SslMode:  "disable",
	}
	configFile, err := os.CreateTemp("", "config.json")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	rdsFile, err := os.CreateTemp("", "rds_credentials.json")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	defer func() {
		_ = os.Remove(configFile.Name())
		_ = os.Remove(rdsFile.Name())
	}()
	err = json.NewEncoder(configFile).Encode(ingesterConfig)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	err = json.NewEncoder(rdsFile).Encode(rdsCredentials)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	// FIXME remove
	dat, _ := os.ReadFile(configFile.Name())
	t.Log(string(dat))
	dat, _ = os.ReadFile(rdsFile.Name())
	t.Log(string(dat))
	ingesterC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "quay.io/armosec/event-ingester-service:rc-v0.0.196-7158453262",
			Env: map[string]string{
				"CONFIG_FILE":               "/config.json",
				"PG_CREDS_RELOAD_FROM_FILE": "/rds_credentials.json",
			},
			Files: []testcontainers.ContainerFile{
				{HostFilePath: configFile.Name(), ContainerFilePath: "/config.json", FileMode: 0o644},
				{HostFilePath: rdsFile.Name(), ContainerFilePath: "/rds_credentials.json", FileMode: 0o644},
			},
			Networks:   []string{backendNetworkName},
			WaitingFor: wait.ForLog("Connection is ready"),
		},
		Started: true,
	})
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	// fake websocket
	clientConn, serverConn := net.Pipe()
	// client side
	clientCfg, err := config.LoadConfig("../configuration/client")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	clusterConfig := &rest.Config{}
	clusterConfig.Host, err = kwokC.Endpoint(ctx, "")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	clientAdapter := incluster.NewInClusterAdapter(clientCfg.InCluster, dynamic.NewForConfigOrDie(clusterConfig))
	client := NewSynchronizerClient(ctx, clientAdapter, clientConn)
	// server side
	serverCfg, err := config.LoadConfig("../configuration/server")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	serverCfg.Backend.PulsarConfig.URL, err = pulsarC.PortEndpoint(ctx, "6650", "pulsar")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	serverCfg.Backend.PulsarConfig.AdminUrl, err = pulsarC.PortEndpoint(ctx, "8080", "http")
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	pulsarClient, err := pulsarconnector.NewClient(
		pulsarconnector.WithConfig(serverCfg.Backend.PulsarConfig),
	)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	pulsarProducer, err := backend.NewPulsarMessageProducer(serverCfg, pulsarClient)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	pulsarConsumer, err := backend.NewPulsarMessageConsumer(serverCfg, pulsarClient)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	serverAdapter := backend.NewBackendAdapter(ctx, pulsarProducer, pulsarConsumer)
	server := NewSynchronizerServer(ctx, serverAdapter, serverConn)
	// start both sides
	go func() {
		_ = client.Start(ctx)
	}()
	go func() {
		_ = server.Start(ctx)
	}()
	return ctx, []testcontainers.Container{ingesterC, kwokC, postgresC, pulsarC, s3C}, backendNetwork, kubernetes.NewForConfigOrDie(clusterConfig), pulsarClient
}

func TestSynchronizer_ObjectAdded(t *testing.T) {
	ctx, containers, network, k8sclient, pulsarClient := initIntegrationTest(t)
	// add pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
		},
	}
	_, err := k8sclient.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	time.Sleep(30 * time.Second)
	// check object added
	//serverObj, ok := serverAdapter.Resources[kindDeployment.String()]
	//assert.True(t, ok)
	//assert.Equal(t, object, serverObj)
	// tear down
	pulsarClient.Close()
	for _, c := range containers {
		if err := c.Terminate(ctx); err != nil {
			panic(err)
		}
	}
	_ = network.Remove(ctx)
}

func TestSynchronizer_ObjectDeleted(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindDeployment.String()] = object
	serverAdapter.Resources[kindDeployment.String()] = object
	// delete object
	err := clientAdapter.TestCallDeleteObject(ctx, kindDeployment)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	time.Sleep(1 * time.Second)
	// check object deleted
	_, ok := serverAdapter.Resources[kindDeployment.String()]
	assert.False(t, ok)
}

func TestSynchronizer_ObjectModified(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindDeployment.String()] = object
	serverAdapter.Resources[kindDeployment.String()] = object
	// modify object
	err := clientAdapter.TestCallPutOrPatch(ctx, kindDeployment, object, objectClientV2)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	time.Sleep(1 * time.Second)
	// check object modified
	serverObj, ok := serverAdapter.Resources[kindDeployment.String()]
	assert.True(t, ok)
	assert.Equal(t, objectClientV2, serverObj)
}

func TestSynchronizer_ObjectModifiedOnBothSides(t *testing.T) {
	ctx, clientAdapter, serverAdapter := initTest(t)
	// pre: add object
	clientAdapter.Resources[kindKnownServers.String()] = object
	serverAdapter.Resources[kindKnownServers.String()] = object
	// manually modify object on server (PutObject message will be sent later)
	serverAdapter.Resources[kindKnownServers.String()] = objectServerV2
	// we create a race condition here
	// object is modified on client, but we don't know about server modification
	err := clientAdapter.TestCallPutOrPatch(ctx, kindKnownServers, object, objectClientV2)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	// server message arrives just now on client
	err = clientAdapter.PutObject(ctx, kindKnownServers, objectServerV2)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	time.Sleep(1 * time.Second)
	// check both sides have the one from the server
	clientObj, ok := clientAdapter.Resources[kindKnownServers.String()]
	assert.True(t, ok)
	assert.Equal(t, objectServerV2, clientObj)
	serverObj, ok := clientAdapter.Resources[kindKnownServers.String()]
	assert.True(t, ok)
	assert.Equal(t, objectServerV2, serverObj)
}
