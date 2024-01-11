//go:build integration
// +build integration

package core

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	"k8s.io/client-go/tools/clientcmd"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/armosec/armoapi-go/identifiers"
	"github.com/armosec/armosec-infra/resourceprocessor"
	"github.com/armosec/armosec-infra/s3connector"
	eventingester "github.com/armosec/event-ingester-service/ingesters/synchronizer_ingester"
	ingesterutils "github.com/armosec/event-ingester-service/utils"
	postgresConnector "github.com/armosec/postgres-connector"
	postgresconnectordal "github.com/armosec/postgres-connector/dal"
	migration "github.com/armosec/postgres-connector/migration"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/adapters/backend/v1"
	"github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/wait"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	account   = "11111111-2222-3333-4444-555555555555"
	cluster   = "cluster"
	namespace = "default"
	name      = "test"
)

var (
	cm = &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Data: map[string]string{"test": "test"},
	}
	deploy = &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "test"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
				},
			},
		},
	}
	pod = &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
		},
	}
	podDesignators = identifiers.PortalDesignator{
		Attributes: map[string]string{
			"apiVersion":   "v1",
			"cluster":      cluster,
			"customerGUID": account,
			"namespace":    namespace,
			"kind":         "Pod",
			"name":         name,
			"syncKind":     "/v1/pods",
		},
	}
	sa = &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ServiceAccount",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	secret = &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	ss = &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "test"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "test"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
				},
			},
		},
	}
)

type Test struct {
	ctx                context.Context
	containers         map[string]testcontainers.Container
	files              []string
	clientAdapter      *incluster.Adapter
	k8sclient          kubernetes.Interface
	pulsarClient       pulsarconnector.Client
	processor          *resourceprocessor.KubernetesResourceProcessor
	s3                 *s3connector.S3Mock
	synchronizerClient *Synchronizer
	synchronizerServer *Synchronizer
}

func randomPorts(n int) []string {
	lowPort := 32768
	highPort := 61000
	ports := mapset.NewSet[string]()
	for {
		// random port number between lowPort and highPort
		port := strconv.Itoa(rand.Intn(highPort-lowPort+1) + lowPort)
		// try to listen on the port
		listener, err := net.Listen("tcp", "0.0.0.0:"+port)
		_ = listener.Close()
		if err == nil && !ports.Contains(port) {
			// port is available
			ports.Add(port)
		}
		if ports.Cardinality() > n-1 {
			break
		}
	}
	return ports.ToSlice()
}

func initIntegrationTest(t *testing.T) *Test {
	ctx := context.WithValue(context.TODO(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: account,
		Cluster: cluster,
	})
	err := logger.L().SetLevel(helpers.DebugLevel.String())
	require.NoError(t, err)
	// generate some random ports
	ports := randomPorts(3)
	// k3s
	k3sC, err := k3s.RunContainer(ctx,
		testcontainers.WithImage("docker.io/rancher/k3s:v1.27.1-k3s1"),
	)
	require.NoError(t, err)
	// pulsar
	pulsarC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apachepulsar/pulsar:2.11.0",
			Cmd:          []string{"bin/pulsar", "standalone"},
			ExposedPorts: []string{ports[0] + ":6650/tcp", ports[1] + ":8080/tcp"},
			WaitingFor:   wait.ForExposedPort(),
		},
		Started: true,
	})
	require.NoError(t, err)
	// postgres
	postgresC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16.1",
			Env: map[string]string{
				"POSTGRES_DB":       "main_db",
				"POSTGRES_USER":     "admin",
				"POSTGRES_PASSWORD": "admin",
			},
			ExposedPorts: []string{ports[2] + ":5432/tcp"},
			WaitingFor:   wait.ForExposedPort(),
		},
		Started: true,
	})
	require.NoError(t, err)
	pgHostPort, err := postgresC.PortEndpoint(ctx, "5432", "")
	require.NoError(t, err)
	// ingester
	t.Setenv("CONFIG_FILE", "../configuration/ingester/config.json")
	ingesterConf := ingesterutils.GetConfig()
	ingesterConf.Postgres.Host = strings.Split(pgHostPort, ":")[0]
	ingesterConf.Postgres.Port = strings.Split(pgHostPort, ":")[1]
	ingesterConf.Pulsar.URL, err = pulsarC.PortEndpoint(ctx, "6650", "pulsar")
	require.NoError(t, err)
	ingesterConf.Pulsar.AdminUrl, err = pulsarC.PortEndpoint(ctx, "8080", "http")
	require.NoError(t, err)
	rdsCredentials := postgresConnector.PostgresConfig{
		Host:     strings.Split(pgHostPort, ":")[0],
		Port:     strings.Split(pgHostPort, ":")[1],
		User:     "admin",
		Password: "admin",
		DB:       "main_db",
		SslMode:  "disable",
	}
	rdsFile, err := os.CreateTemp("", "rds_credentials.json")
	require.NoError(t, err)
	err = json.NewEncoder(rdsFile).Encode(rdsCredentials)
	require.NoError(t, err)
	pgOpts := []postgresConnector.ConnectorOption{
		postgresConnector.WithReloadFromFile(rdsFile.Name()),
		postgresConnector.WithUseDebugConnection(ingesterConf.Logger.Level == "debug"),
	}
	ingesterPgClient := postgresConnector.NewPostgresClient(*ingesterConf.Postgres, pgOpts...)
	err = ingesterPgClient.Connect()
	require.NoError(t, err)
	// run migrations
	migration.DbMigrations(ingesterPgClient.GetClient())
	pulsarClient, err := pulsarconnector.NewClient(
		pulsarconnector.WithConfig(&ingesterConf.Pulsar),
		pulsarconnector.WithRetryAttempts(20),
		pulsarconnector.WithRetryMaxDelay(3*time.Second),
	)
	require.NoError(t, err)
	ingesterProducer, err := eventingester.NewPulsarProducer(pulsarClient, ingesterConf.SynchronizerIngesterConfig.Topic)
	require.NoError(t, err)
	s3 := s3connector.NewS3Mock()
	ingesterProcessor := resourceprocessor.NewKubernetesResourceProcessor(&s3, postgresconnectordal.NewPostgresDAL(ingesterPgClient))
	ingester := eventingester.NewSynchronizerWithProcessor(ingesterProducer, pulsarClient, *ingesterConf.SynchronizerIngesterConfig, ingesterPgClient, ingesterProcessor)
	go ingester.ConsumeSynchronizerMessages(ctx)
	// fake websocket
	clientConn, serverConn := net.Pipe()
	// client side
	clientCfg, err := config.LoadConfig("../configuration/client")
	require.NoError(t, err)
	kubeConfigYaml, err := k3sC.GetKubeConfig(ctx)
	require.NoError(t, err)
	clusterConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeConfigYaml)
	require.NoError(t, err)
	clientAdapter := incluster.NewInClusterAdapter(clientCfg.InCluster, dynamic.NewForConfigOrDie(clusterConfig))
	newConn := func() (net.Conn, error) {
		return clientConn, nil
	}
	synchronizerClient := NewSynchronizerClient(ctx, clientAdapter, clientConn, newConn)
	// server side
	serverCfg, err := config.LoadConfig("../configuration/server")
	require.NoError(t, err)
	serverCfg.Backend.PulsarConfig.URL, err = pulsarC.PortEndpoint(ctx, "6650", "pulsar")
	require.NoError(t, err)
	serverCfg.Backend.PulsarConfig.AdminUrl, err = pulsarC.PortEndpoint(ctx, "8080", "http")
	require.NoError(t, err)
	pulsarProducer, err := backend.NewPulsarMessageProducer(serverCfg, pulsarClient)
	require.NoError(t, err)
	pulsarConsumer, err := backend.NewPulsarMessageConsumer(serverCfg, pulsarClient)
	require.NoError(t, err)
	serverAdapter := backend.NewBackendAdapter(ctx, pulsarProducer, pulsarConsumer)
	synchronizerServer := NewSynchronizerServer(ctx, serverAdapter, serverConn)
	// start both sides
	go func() {
		_ = synchronizerClient.Start(ctx)
	}()
	go func() {
		_ = synchronizerServer.Start(ctx)
	}()
	return &Test{
		ctx:                ctx,
		containers:         map[string]testcontainers.Container{"k3s": k3sC, "postgres": postgresC, "pulsar": pulsarC},
		files:              []string{rdsFile.Name()},
		clientAdapter:      clientAdapter,
		k8sclient:          kubernetes.NewForConfigOrDie(clusterConfig),
		pulsarClient:       pulsarClient,
		processor:          ingesterProcessor,
		s3:                 &s3,
		synchronizerClient: synchronizerClient,
		synchronizerServer: synchronizerServer,
	}
}

func tearDown(td *Test) {
	td.pulsarClient.Close()
	for _, c := range td.containers {
		_ = c.Terminate(td.ctx)
	}
	for _, f := range td.files {
		_ = os.Remove(f)
	}
}

// TestSynchronizer_TC01_InCluster: Initial synchronization of a single entity
func TestSynchronizer_TC01_InCluster(t *testing.T) {
	td := initIntegrationTest(t)
	// add pod to k8s
	_, err := td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	objMetadata, objFound, err := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	assert.Equal(t, podDesignators, objMetadata.Designators)
	// check object in s3
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)
	// compare object in s3 with object in k8s
	k8sPod, err := td.k8sclient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	// workaround for empty TypeMeta (from k3s?)
	k8sPod.TypeMeta.Kind = "Pod"
	k8sPod.TypeMeta.APIVersion = "v1"
	var s3Pod corev1.Pod
	err = json.Unmarshal(bytes, &s3Pod)
	require.NoError(t, err)
	// full pod should not match
	assert.NotEqual(t, k8sPod, &s3Pod)
	// remove managed fields and last-applied-configuration annotation
	k8sPod.SetManagedFields(nil)
	delete(k8sPod.Annotations, "kubectl.kubernetes.io/last-applied-configuration")
	assert.Equal(t, k8sPod, &s3Pod)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC01_Backend: Initial synchronization of a single entity
func TestSynchronizer_TC01_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := eventingester.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	bytes, err := json.Marshal(cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, account, cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in k8s
	k8sCm, err := td.k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test", k8sCm.Data["test"])
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC02_InCluster: Delta synchronization of a single entity
func TestSynchronizer_TC02_InCluster(t *testing.T) {
	td := initIntegrationTest(t)
	// add pod to k8s
	_, err := td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// modify pod in k8s
	k8sPod, err := td.k8sclient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sPod.Spec.Containers[0].Image = "nginx2"
	_, err = td.k8sclient.CoreV1().Pods(namespace).Update(context.TODO(), k8sPod, metav1.UpdateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// get object path from postgres
	objMetadata, _, _ := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// check object in s3
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3Pod corev1.Pod
	err = json.Unmarshal(bytes, &s3Pod)
	require.NoError(t, err)
	assert.Equal(t, "nginx2", s3Pod.Spec.Containers[0].Image)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC02_Backend: Delta synchronization of a single entity
func TestSynchronizer_TC02_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := eventingester.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	bytes, err := json.Marshal(cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, account, cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// modify cm via pulsar message (we don't send patches from backend)
	cm2 := cm.DeepCopy()
	cm2.Data["test"] = "test2"
	bytes, err = json.Marshal(cm2)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, account, cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in k8s
	k8sCm, err := td.k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test2", k8sCm.Data["test"])
	// tear down
	tearDown(td)
}

// FIXME phase 2, bi-directional synchronization needed
// TestSynchronizer_TC03_InCluster: Conflict resolution for a single entity
//func TestSynchronizer_TC03_InCluster(t *testing.T) {
//	td := initIntegrationTest(t)
//
//	// tear down
//	tearDown(td)
//}

// FIXME phase 2, bi-directional synchronization needed
// TestSynchronizer_TC03_Backend: Conflict resolution for a single entity
//func TestSynchronizer_TC03_Backend(t *testing.T) {
//	td := initIntegrationTest(t)
//
//	// tear down
//	tearDown(td)
//}

// TestSynchronizer_TC04_InCluster: Deletion of a single entity
func TestSynchronizer_TC04_InCluster(t *testing.T) {
	td := initIntegrationTest(t)
	// add pod to k8s
	_, err := td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	// delete pod from k8s
	err = td.k8sclient.CoreV1().Pods(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object not in postgres
	_, objFound, err = td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	assert.Error(t, err)
	assert.False(t, objFound)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC04_Backend: Deletion of a single entity
func TestSynchronizer_TC04_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := eventingester.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	bytes, err := json.Marshal(cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, account, cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in k8s
	k8sCm, err := td.k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test", k8sCm.Data["test"])
	// delete cm via pulsar message
	err = pulsarProducer.SendDeleteObjectMessage(td.ctx, account, cluster, "/v1/configmaps", namespace, name, "", 0)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object not in k8s
	_, err = td.k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.Error(t, err)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC05_InCluster: Synchronizing entities with different types
func TestSynchronizer_TC05_InCluster(t *testing.T) {
	td := initIntegrationTest(t)
	// add objects to k8s
	_, err := td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = td.k8sclient.AppsV1().Deployments(namespace).Create(context.TODO(), deploy, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = td.k8sclient.AppsV1().StatefulSets(namespace).Create(context.TODO(), ss, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check objects in postgres
	for _, kind := range []string{"/v1/pods", "apps/v1/deployments", "apps/v1/statefulsets"} {
		_, objFound, err := td.processor.GetObjectFromPostgres(account, cluster, kind, namespace, name)
		assert.NoError(t, err)
		assert.True(t, objFound)
	}
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC05_Backend: Synchronizing entities with different types
func TestSynchronizer_TC05_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add objects to backend via pulsar message
	pulsarProducer, err := eventingester.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	objs := map[string]any{
		"/v1/configmaps":      cm,
		"/v1/secrets":         secret,
		"/v1/serviceaccounts": sa,
	}
	for kind, obj := range objs {
		bytes, err := json.Marshal(obj)
		require.NoError(t, err)
		err = pulsarProducer.SendPutObjectMessage(td.ctx, account, cluster, kind, namespace, name, "", 0, bytes)
		require.NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	// check objects in k8s
	_, err = td.k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	_, err = td.k8sclient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	_, err = td.k8sclient.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC06: Invalid source data
// it is similar to TC-02_InCluster, but altering the reference object in client before generating a patch
func TestSynchronizer_TC06(t *testing.T) {
	td := initIntegrationTest(t)
	// add pod to k8s
	_, err := td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// prepare to alter shadow object in client
	podClient, err := td.clientAdapter.GetClient(domain.KindName{Kind: domain.KindFromString(context.TODO(), "/v1/pods")})
	require.NoError(t, err)
	// modify pod in k8s
	k8sPod, err := td.k8sclient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sPod.Spec.Containers[0].Image = "nginx2"
	// alter shadow object in client before updating k8s, the patch won't include the image change
	podClient.(*incluster.Client).ShadowObjects[name], err = json.Marshal(k8sPod)
	require.NoError(t, err)
	_, err = td.k8sclient.CoreV1().Pods(namespace).Update(context.TODO(), k8sPod, metav1.UpdateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// get object path from postgres
	objMetadata, _, _ := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// check object in s3
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3Pod corev1.Pod
	err = json.Unmarshal(bytes, &s3Pod)
	require.NoError(t, err)
	assert.Equal(t, "nginx2", s3Pod.Spec.Containers[0].Image)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC07: Invalid target data
// it is similar to TC-02_InCluster, but altering the reference object in s3 before applying a patch
func TestSynchronizer_TC07(t *testing.T) {
	td := initIntegrationTest(t)
	// add pod to k8s
	_, err := td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// get object path from postgres
	objMetadata, _, _ := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// alter object in s3, the patched object won't match the checksum
	_, err = td.s3.UpdateObject(objPath, strings.NewReader("{}"))
	require.NoError(t, err)
	// modify pod in k8s
	k8sPod, err := td.k8sclient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sPod.Spec.Containers[0].Image = "nginx2"
	_, err = td.k8sclient.CoreV1().Pods(namespace).Update(context.TODO(), k8sPod, metav1.UpdateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in s3
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3Pod corev1.Pod
	err = json.Unmarshal(bytes, &s3Pod)
	require.NoError(t, err)
	assert.Equal(t, "nginx2", s3Pod.Spec.Containers[0].Image)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC08: Communication failure with the backend
// it is similar to TC-01_InCluster, but the communication between synchronizers is interrupted
func TestSynchronizer_TC08(t *testing.T) {
	td := initIntegrationTest(t)
	// kill network connection on client side
	dead, _ := net.Pipe()
	err := dead.Close()
	require.NoError(t, err)
	*td.synchronizerClient.Conn = dead
	// add pod to k8s
	_, err = td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC09: Communication failure with pulsar
// it is similar to TC-01_InCluster, but the communication with pulsar container is interrupted
// This test works because the pulsar client retries the send message operation - we don't plan to implement other mitigations
// and will rely on the initial sync message to recover from any lost messages (we could even implement a manual trigger for the full sync)
func TestSynchronizer_TC09(t *testing.T) {
	td := initIntegrationTest(t)
	// stop pulsar
	err := td.containers["pulsar"].Stop(td.ctx, nil)
	require.NoError(t, err)
	// add pod to k8s
	_, err = td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// restart pulsar
	err = td.containers["pulsar"].Start(td.ctx)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC10: Communication failure with postgres
// it is similar to TC-01_InCluster, but the communication with postgres container is interrupted
// we check the message is again available in pulsar after nack
func TestSynchronizer_TC10(t *testing.T) {
	td := initIntegrationTest(t)
	// stop postgres
	err := td.containers["postgres"].Stop(td.ctx, nil)
	require.NoError(t, err)
	// add pod to k8s
	_, err = td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// restart postgres
	err = td.containers["postgres"].Start(td.ctx)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC11: Communication failure with S3
// it is similar to TC-01_InCluster, but the communication with S3 is interrupted
// we check the message is again available in pulsar after nack
func TestSynchronizer_TC11(t *testing.T) {
	td := initIntegrationTest(t)
	// disable S3Mock
	td.s3.SetReturnError(true)
	// add pod to k8s
	_, err := td.k8sclient.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	time.Sleep(5 * time.Second)
	// re-enable S3Mock
	td.s3.SetReturnError(false)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(account, cluster, "/v1/pods", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	// tear down
	tearDown(td)
}
