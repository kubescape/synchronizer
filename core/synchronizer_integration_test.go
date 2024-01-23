//go:build integration
// +build integration

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	ingesterutils "github.com/armosec/event-ingester-service/utils"
	postgresConnector "github.com/armosec/postgres-connector"
	postgresconnectordal "github.com/armosec/postgres-connector/dal"
	migration "github.com/armosec/postgres-connector/migration"
	"github.com/cenkalti/backoff/v4"

	"github.com/kubescape/storage/pkg/apis/softwarecomposition/v1beta1"
	"github.com/kubescape/synchronizer/adapters/backend/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	"github.com/testcontainers/testcontainers-go/wait"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/ptr"

	"github.com/armosec/armoapi-go/identifiers"
	"github.com/armosec/armosec-infra/resourceprocessor"
	"github.com/armosec/armosec-infra/s3connector"
	eventingester "github.com/armosec/event-ingester-service/ingesters/synchronizer_ingester"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	spdxv1beta1client "github.com/kubescape/storage/pkg/generated/clientset/versioned/typed/softwarecomposition/v1beta1"
	"github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/domain"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	apiregistrationclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
)

const (
	namespace = "default"
	name      = "test"
)

type Test struct {
	ctx          context.Context
	containers   map[string]testcontainers.Container
	files        []string
	clusters     []TestKubernetesCluster
	pulsarClient pulsarconnector.Client
	processor    *resourceprocessor.KubernetesResourceProcessor
	s3           *s3connector.S3Mock
	syncServers  []TestSynchronizerServer
}

type TestSynchronizerServer struct {
	serverUrl                 string
	syncServer                *Synchronizer
	syncServerContextCancelFn context.CancelFunc
	conn                      net.Conn
	pulsarProducer            *backend.PulsarMessageProducer
	pulsarReader              *backend.PulsarMessageReader
}

type TestKubernetesCluster struct {
	ctx           context.Context
	account       string
	cluster       string
	clusterConfig *rest.Config
	k3sC          *k3s.K3sContainer
	k8sclient     kubernetes.Interface
	storageclient *spdxv1beta1client.SpdxV1beta1Client

	// objects
	applicationprofile            *v1beta1.ApplicationProfile
	applicationprofileDesignators identifiers.PortalDesignator
	cm                            *corev1.ConfigMap
	deploy                        *appsv1.Deployment
	sa                            *corev1.ServiceAccount
	secret                        *corev1.Secret
	ss                            *appsv1.StatefulSet

	// synchronizer
	syncClient                *Synchronizer
	syncClientAdapter         *incluster.Adapter
	syncClientContextCancelFn context.CancelFunc

	clientConn net.Conn
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
		if err != nil {
			// try again
			continue
		}
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
func createK8sCluster(t *testing.T, cluster, account string) *TestKubernetesCluster {
	ctx := context.WithValue(context.TODO(), domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: account,
		Cluster: cluster,
	})

	var (
		applicationprofile = &v1beta1.ApplicationProfile{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "spdx.softwarecomposition.kubescape.io/v1beta1",
				Kind:       "ApplicationProfile",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: v1beta1.ApplicationProfileSpec{
				Containers: []v1beta1.ApplicationProfileContainer{{
					Name: "nginx",
					Execs: []v1beta1.ExecCalls{
						{Path: "/usr/sbin/nginx", Args: []string{"-g", "/usr/sbin/nginx", "daemon off;"}},
					},
				}},
			},
		}
		applicationprofileDesignators = identifiers.PortalDesignator{
			Attributes: map[string]string{
				"apiVersion":   "spdx.softwarecomposition.kubescape.io/v1beta1",
				"cluster":      cluster,
				"customerGUID": account,
				"namespace":    namespace,
				"kind":         "ApplicationProfile",
				"name":         name,
				"syncKind":     "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles",
			},
		}
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

	// k3s
	k3sC, err := k3s.RunContainer(ctx,
		testcontainers.WithImage("docker.io/rancher/k3s:v1.27.9-k3s1"),
	)
	require.NoError(t, err)
	kubeConfigYaml, err := k3sC.GetKubeConfig(ctx)
	require.NoError(t, err)
	clusterConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeConfigYaml)
	require.NoError(t, err)
	k8sclient := kubernetes.NewForConfigOrDie(clusterConfig)
	// apiservice
	regclient := apiregistrationclient.NewForConfigOrDie(clusterConfig)
	_, err = regclient.APIServices().Create(context.TODO(),
		&apiregistrationv1.APIService{
			ObjectMeta: metav1.ObjectMeta{Name: "v1beta1.spdx.softwarecomposition.kubescape.io"},
			Spec: apiregistrationv1.APIServiceSpec{
				InsecureSkipTLSVerify: true,
				Group:                 "spdx.softwarecomposition.kubescape.io",
				GroupPriorityMinimum:  1000,
				VersionPriority:       15,
				Version:               "v1beta1",
				Service: &apiregistrationv1.ServiceReference{
					Name:      "storage",
					Namespace: "kubescape",
				},
			},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// kubescape namespace
	_, err = k8sclient.CoreV1().Namespaces().Create(context.TODO(),
		&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "kubescape"},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage service
	storageLabels := map[string]string{"app.kubernetes.io/name": "storage"}
	_, err = k8sclient.CoreV1().Services("kubescape").Create(context.TODO(),
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "storage"},
			Spec: corev1.ServiceSpec{
				Ports:    []corev1.ServicePort{{Port: 443, Protocol: "TCP", TargetPort: intstr.FromInt32(8443)}},
				Selector: storageLabels,
			},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage configmap
	_, err = k8sclient.CoreV1().ConfigMaps("kubescape").Create(context.TODO(),
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "ks-cloud-config"},
			Data: map[string]string{
				"clusterData": `{"clusterName":"k3s"}`,
			},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage serviceaccount
	_, err = k8sclient.CoreV1().ServiceAccounts("kubescape").Create(context.TODO(),
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: "storage"},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage rolebinding
	_, err = k8sclient.RbacV1().RoleBindings("kube-system").Create(context.TODO(),
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "storage-auth-reader"},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "Role",
				Name:     "extension-apiserver-authentication-reader",
			},
			Subjects: []rbacv1.Subject{{
				Kind:      "ServiceAccount",
				Name:      "storage",
				Namespace: "kubescape",
			}},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage clusterrole
	_, err = k8sclient.RbacV1().ClusterRoles().Create(context.TODO(),
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "storage"},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{""}, Resources: []string{"namespaces"}, Verbs: []string{"get", "watch", "list"}},
				{APIGroups: []string{"admissionregistration.k8s.io"}, Resources: []string{"mutatingwebhookconfigurations", "validatingwebhookconfigurations"}, Verbs: []string{"get", "watch", "list"}},
				{APIGroups: []string{"flowcontrol.apiserver.k8s.io"}, Resources: []string{"prioritylevelconfigurations", "flowschemas"}, Verbs: []string{"get", "watch", "list"}},
			},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage clusterrolebinding
	_, err = k8sclient.RbacV1().ClusterRoleBindings().Create(context.TODO(),
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "storage"},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "storage",
			},
			Subjects: []rbacv1.Subject{{
				Kind:      "ServiceAccount",
				Name:      "storage",
				Namespace: "kubescape",
			}},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage clusterrolebinding 2
	_, err = k8sclient.RbacV1().ClusterRoleBindings().Create(context.TODO(),
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "storage:system:auth-delegator"},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     "system:auth-delegator",
			},
			Subjects: []rbacv1.Subject{{
				Kind:      "ServiceAccount",
				Name:      "storage",
				Namespace: "kubescape",
			}},
		}, metav1.CreateOptions{})
	require.NoError(t, err)
	// storage deployment
	_, err = k8sclient.AppsV1().Deployments("kubescape").Create(context.TODO(),
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "storage", Labels: storageLabels},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: storageLabels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: storageLabels},
					Spec: corev1.PodSpec{
						ServiceAccountName: "storage",
						SecurityContext: &corev1.PodSecurityContext{
							RunAsUser: ptr.To(int64(65532)),
							FSGroup:   ptr.To(int64(65532)),
						},
						Containers: []corev1.Container{{
							Name:  "apiserver",
							Image: "quay.io/kubescape/storage:v0.0.57",
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data"},
								{Name: "ks-cloud-config", MountPath: "/etc/config"},
							},
						}},
						Volumes: []corev1.Volume{
							{Name: "data", VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							}},
							{Name: "ks-cloud-config", VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "ks-cloud-config"},
									Items: []corev1.KeyToPath{
										{Key: "clusterData", Path: "clusterData.json"},
									},
								},
							}},
						},
					},
				},
			},
		}, metav1.CreateOptions{})
	require.NoError(t, err)

	return &TestKubernetesCluster{
		account:                       account,
		cluster:                       cluster,
		ctx:                           ctx,
		clusterConfig:                 clusterConfig,
		k3sC:                          k3sC,
		k8sclient:                     k8sclient,
		applicationprofile:            applicationprofile,
		applicationprofileDesignators: applicationprofileDesignators,
		cm:                            cm,
		deploy:                        deploy,
		sa:                            sa,
		secret:                        secret,
		ss:                            ss,
	}
}

func createPulsar(t *testing.T, ctx context.Context, brokerPort, adminPort string) (pulsarC testcontainers.Container, pulsarUrl, pulsarAdminUrl string) {
	pulsarC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apachepulsar/pulsar:2.11.0",
			Cmd:          []string{"bin/pulsar", "standalone"},
			ExposedPorts: []string{brokerPort + ":6650/tcp", adminPort + ":8080/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForExposedPort(),
				wait.ForHTTP("/admin/v2/clusters").WithPort("8080/tcp").WithResponseMatcher(func(r io.Reader) bool {
					respBytes, _ := io.ReadAll(r)
					resp := string(respBytes)
					return strings.Contains(resp, `["standalone"]`)
				}),
			),
		},
		Started: true,
	})
	require.NoError(t, err)
	pulsarUrl, err = pulsarC.PortEndpoint(ctx, "6650", "pulsar")
	require.NoError(t, err)
	pulsarAdminUrl, err = pulsarC.PortEndpoint(ctx, "8080", "http")
	require.NoError(t, err)
	return pulsarC, pulsarUrl, pulsarAdminUrl
}

func createPostgres(t *testing.T, ctx context.Context, port string) (postgresC testcontainers.Container, pgHostPort string) {
	// postgres
	postgresC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16.1",
			Env: map[string]string{
				"POSTGRES_DB":       "main_db",
				"POSTGRES_USER":     "admin",
				"POSTGRES_PASSWORD": "admin",
			},
			ExposedPorts: []string{port + ":5432/tcp"},
			WaitingFor:   wait.ForExposedPort(),
		},
		Started: true,
	})
	require.NoError(t, err)
	pgHostPort, err = postgresC.PortEndpoint(ctx, "5432", "")
	require.NoError(t, err)
	return postgresC, pgHostPort
}

func waitForStorage(t *testing.T, cluster *TestKubernetesCluster) {
	// wait until storage is ready
	storageclient := spdxv1beta1client.NewForConfigOrDie(cluster.clusterConfig)
	cluster.storageclient = storageclient

	err := backoff.RetryNotify(func() error {
		_, err := storageclient.ApplicationProfiles(namespace).Create(context.TODO(), cluster.applicationprofile, metav1.CreateOptions{})
		return err
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(5*time.Second), 20), func(err error, d time.Duration) {
		logger.L().Info("waiting for storage to be ready", helpers.Error(err), helpers.String("retry in", d.String()))
	})
	require.NoError(t, err)
}

func createAndStartSynchronizerClient(t *testing.T, cluster *TestKubernetesCluster, clientConn net.Conn, syncServer *TestSynchronizerServer) {
	// client side
	clientCfg, err := config.LoadConfig("../configuration/client")
	require.NoError(t, err)

	// set cluster config
	clientCfg.InCluster.ClusterName = cluster.cluster
	clientCfg.InCluster.Account = cluster.account
	clientCfg.InCluster.ServerUrl = syncServer.serverUrl

	clientAdapter := incluster.NewInClusterAdapter(clientCfg.InCluster, dynamic.NewForConfigOrDie(cluster.clusterConfig))
	newConn := func() (net.Conn, error) {
		return clientConn, nil
	}

	ctx, cancel := context.WithCancel(cluster.ctx)

	cluster.syncClient = NewSynchronizerClient(ctx, clientAdapter, clientConn, newConn)
	cluster.syncClientAdapter = clientAdapter
	cluster.clientConn = clientConn
	cluster.syncClientContextCancelFn = cancel

	// wait until storage is ready
	waitForStorage(t, cluster)

	// start synchronizer client
	go func() {
		_ = cluster.syncClient.Start(ctx)
	}()
}

func createAndStartSynchronizerServer(t *testing.T, pulsarUrl, pulsarAdminUrl string, pulsarClient pulsarconnector.Client, serverConn net.Conn, serverPort string, cluster *TestKubernetesCluster) *TestSynchronizerServer {
	// server side
	serverCfg, err := config.LoadConfig("../configuration/server")
	require.NoError(t, err)

	// set server config
	serverCfg.Backend.Port, _ = strconv.Atoi(serverPort)
	serverCfg.Backend.Subscription = serverCfg.Backend.Subscription + strconv.Itoa(rand.Intn(1000))

	serverCfg.Backend.PulsarConfig.URL = pulsarUrl
	serverCfg.Backend.PulsarConfig.AdminUrl = pulsarAdminUrl
	pulsarProducer, err := backend.NewPulsarMessageProducer(serverCfg, pulsarClient)
	require.NoError(t, err)
	pulsarReader, err := backend.NewPulsarMessageReader(serverCfg, pulsarClient)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(cluster.ctx)
	serverAdapter := backend.NewBackendAdapter(ctx, pulsarProducer, pulsarReader)
	synchronizerServer := NewSynchronizerServer(ctx, serverAdapter, serverConn)

	// start server
	go func() {
		_ = synchronizerServer.Start(ctx)
	}()

	return &TestSynchronizerServer{
		pulsarProducer:            pulsarProducer,
		pulsarReader:              pulsarReader,
		syncServerContextCancelFn: cancel,
		serverUrl:                 fmt.Sprintf("ws://127.0.0.1:%s/", serverPort),
		syncServer:                synchronizerServer,
		conn:                      serverConn,
	}
}

func initIntegrationTest(t *testing.T) *Test {
	ctx := context.TODO()

	err := logger.L().SetLevel(helpers.DebugLevel.String())
	require.NoError(t, err)

	// create k8s cluster
	cluster_1 := createK8sCluster(t, "cluster1", "b486ba4e-ffaa-4cd4-b885-b6d26cd13193")
	cluster_2 := createK8sCluster(t, "cluster2", "0757d22d-a9c1-4ca3-87b6-f2236f7f5885")

	// generate some random ports
	ports := randomPorts(5)

	// pulsar
	pulsarC, pulsarUrl, pulsarAdminUrl := createPulsar(t, ctx, ports[0], ports[1])

	// postgres
	postgresC, pgHostPort := createPostgres(t, ctx, ports[2])

	// ingester
	t.Setenv("CONFIG_FILE", "../configuration/ingester/config.json")
	ingesterConf := ingesterutils.GetConfig()
	ingesterConf.Postgres.Host = strings.Split(pgHostPort, ":")[0]
	ingesterConf.Postgres.Port = strings.Split(pgHostPort, ":")[1]
	ingesterConf.Pulsar.URL = pulsarUrl
	ingesterConf.Pulsar.AdminUrl = pulsarAdminUrl
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
	clientConn1, serverConn1 := net.Pipe() // client1 -> server1
	clientConn2, serverConn2 := net.Pipe() // client2 -> server2

	// synchronizer servers
	synchronizerServer1 := createAndStartSynchronizerServer(t, pulsarUrl, pulsarAdminUrl, pulsarClient, serverConn1, ports[3], cluster_1)
	synchronizerServer2 := createAndStartSynchronizerServer(t, pulsarUrl, pulsarAdminUrl, pulsarClient, serverConn2, ports[4], cluster_2)

	// synchronizer clients
	createAndStartSynchronizerClient(t, cluster_1, clientConn1, synchronizerServer1)
	createAndStartSynchronizerClient(t, cluster_2, clientConn2, synchronizerServer2)

	return &Test{
		ctx: ctx,
		containers: map[string]testcontainers.Container{
			"k3s1":     cluster_1.k3sC,
			"k3s2":     cluster_2.k3sC,
			"postgres": postgresC,
			"pulsar":   pulsarC,
		},
		files:        []string{rdsFile.Name()},
		clusters:     []TestKubernetesCluster{*cluster_1, *cluster_2},
		pulsarClient: pulsarClient,
		processor:    ingesterProcessor,
		s3:           &s3,
		syncServers: []TestSynchronizerServer{
			*synchronizerServer1,
			*synchronizerServer2,
		},
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
	// add applicationprofile to k8s
	_, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	objMetadata, objFound, err := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	assert.Equal(t, td.clusters[0].applicationprofileDesignators, objMetadata.Designators)
	// check object in s3
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)
	// compare object in s3 with object in k8s
	k8sAppProfile, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	// workaround for empty TypeMeta (from k3s?)
	k8sAppProfile.TypeMeta.Kind = "ApplicationProfile"
	k8sAppProfile.TypeMeta.APIVersion = "spdx.softwarecomposition.kubescape.io/v1beta1"
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(bytes, &s3AppProfile)
	require.NoError(t, err)
	// full applicationprofile should not match
	assert.NotEqual(t, k8sAppProfile, &s3AppProfile)
	// remove managed fields and last-applied-configuration annotation
	k8sAppProfile.SetManagedFields(nil)
	delete(k8sAppProfile.Annotations, "kubectl.kubernetes.io/last-applied-configuration")
	assert.Equal(t, k8sAppProfile, &s3AppProfile)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC01_Backend: Initial synchronization of a single entity
func TestSynchronizer_TC01_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := eventingester.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	bytes, err := json.Marshal(td.clusters[0].cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in k8s
	k8sCm, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test", k8sCm.Data["test"])
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC02_InCluster: Delta synchronization of a single entity
func TestSynchronizer_TC02_InCluster(t *testing.T) {
	td := initIntegrationTest(t)
	// add applicationprofile to k8s
	_, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// modify applicationprofile in k8s
	k8sAppProfile, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sAppProfile.Spec.Containers[0].Name = "nginx2"
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Update(context.TODO(), k8sAppProfile, metav1.UpdateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// get object path from postgres
	objMetadata, _, _ := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// check object in s3
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(bytes, &s3AppProfile)
	require.NoError(t, err)
	assert.Equal(t, "nginx2", s3AppProfile.Spec.Containers[0].Name)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC02_Backend: Delta synchronization of a single entity
func TestSynchronizer_TC02_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := eventingester.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	bytes, err := json.Marshal(td.clusters[0].cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// modify cm via pulsar message (we don't send patches from backend)
	cm2 := td.clusters[0].cm.DeepCopy()
	cm2.Data["test"] = "test2"
	bytes, err = json.Marshal(cm2)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in k8s
	k8sCm, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
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
	// add applicationprofile to k8s
	_, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	// delete applicationprofile from k8s
	err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object not in postgres
	_, objFound, err = td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
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
	bytes, err := json.Marshal(td.clusters[0].cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, bytes)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in k8s
	k8sCm, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test", k8sCm.Data["test"])
	// delete cm via pulsar message
	err = pulsarProducer.SendDeleteObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object not in k8s
	_, err = td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.Error(t, err)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC05_InCluster: Synchronizing entities with different types
func TestSynchronizer_TC05_InCluster(t *testing.T) {
	td := initIntegrationTest(t)
	// add objects to k8s
	_, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = td.clusters[0].k8sclient.AppsV1().Deployments(namespace).Create(context.TODO(), td.clusters[0].deploy, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = td.clusters[0].k8sclient.AppsV1().StatefulSets(namespace).Create(context.TODO(), td.clusters[0].ss, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check objects in postgres
	for _, kind := range []string{"spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", "apps/v1/deployments", "apps/v1/statefulsets"} {
		_, objFound, err := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, kind, namespace, name)
		assert.NoError(t, err)
		assert.True(t, objFound)
	}
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC05_Backend: Synchronizing entities with different types
// this test also checks that the synchronizer does not create objects in clusters that are not the source of the message
func TestSynchronizer_TC05_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add objects to backend via pulsar message
	pulsarProducer, err := eventingester.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)

	// cluster 1
	objs_c1 := map[string]any{
		"/v1/configmaps": td.clusters[0].cm,
		"/v1/secrets":    td.clusters[0].secret,
	}
	for kind, obj := range objs_c1 {
		bytes, err := json.Marshal(obj)
		require.NoError(t, err)
		err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, kind, namespace, name, "", 0, bytes)
		require.NoError(t, err)
	}

	// cluster 2
	objs_c2 := map[string]any{
		"/v1/serviceaccounts": td.clusters[1].sa,
	}
	for kind, obj := range objs_c2 {
		bytes, err := json.Marshal(obj)
		require.NoError(t, err)
		err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[1].account, td.clusters[1].cluster, kind, namespace, name, "", 0, bytes)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	// check objects in k8s (cluster 1)
	_, err = td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoErrorf(t, err, "Expected a ConfigMap to be created in cluster 1")
	_, err = td.clusters[0].k8sclient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoErrorf(t, err, "Expected a Secret to be created in cluster 1")
	_, err = td.clusters[0].k8sclient.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.Errorf(t, err, "No ServiceAccounts should exist in cluster 1")

	// check objects in k8s (cluster 2)
	_, err = td.clusters[1].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.Errorf(t, err, "No ConfigMaps should exist in cluster 2")
	_, err = td.clusters[1].k8sclient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.Errorf(t, err, "No Secrets should exist in cluster 2")
	_, err = td.clusters[1].k8sclient.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoErrorf(t, err, "Expected a ServiceAccount to be created in cluster 2")

	// tear down
	tearDown(td)
}

// TestSynchronizer_TC06: Invalid source data
// it is similar to TC-02_InCluster, but altering the reference object in client before generating a patch
func TestSynchronizer_TC06(t *testing.T) {
	td := initIntegrationTest(t)
	// add applicationprofile to k8s
	_, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// prepare to alter shadow object in client
	appClient, err := td.clusters[0].syncClientAdapter.GetClient(domain.KindName{Kind: domain.KindFromString(context.TODO(), "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles")})
	require.NoError(t, err)
	// modify applicationprofile in k8s
	k8sAppProfile, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sAppProfile.Spec.Containers[0].Name = "nginx2"
	// alter shadow object in client before updating k8s, the patch won't include the image change
	appClient.(*incluster.Client).ShadowObjects[name], err = json.Marshal(k8sAppProfile)
	require.NoError(t, err)
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Update(context.TODO(), k8sAppProfile, metav1.UpdateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// get object path from postgres
	objMetadata, _, _ := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// check object in s3
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(bytes, &s3AppProfile)
	require.NoError(t, err)
	assert.Equal(t, "nginx2", s3AppProfile.Spec.Containers[0].Name)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC07: Invalid target data
// it is similar to TC-02_InCluster, but altering the reference object in s3 before applying a patch
func TestSynchronizer_TC07(t *testing.T) {
	td := initIntegrationTest(t)
	// add applicationprofile to k8s
	_, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// get object path from postgres
	objMetadata, _, _ := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// alter object in s3, the patched object won't match the checksum
	_, err = td.s3.UpdateObject(objPath, strings.NewReader("{}"))
	require.NoError(t, err)
	// modify applicationprofile in k8s
	k8sAppProfile, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sAppProfile.Spec.Containers[0].Name = "nginx2"
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Update(context.TODO(), k8sAppProfile, metav1.UpdateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in s3
	bytes, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(bytes, &s3AppProfile)
	require.NoError(t, err)
	assert.Equal(t, "nginx2", s3AppProfile.Spec.Containers[0].Name)
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
	*td.clusters[0].syncClient.Conn = dead
	// add applicationprofile to k8s
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
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
	// add applicationprofile to k8s
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// restart pulsar
	err = td.containers["pulsar"].Start(td.ctx)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
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
	// add applicationprofile to k8s
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// restart postgres
	err = td.containers["postgres"].Start(td.ctx)

	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
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
	// add applicationprofile to k8s
	_, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	time.Sleep(5 * time.Second)
	// re-enable S3Mock
	td.s3.SetReturnError(false)
	time.Sleep(5 * time.Second)
	// check object in postgres
	_, objFound, err := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	assert.NoError(t, err)
	assert.True(t, objFound)
	// tear down
	tearDown(td)
}
