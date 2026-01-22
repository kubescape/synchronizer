//go:build integration

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	configserviceconnector "github.com/armosec/event-ingester-service/config_service_connector"
	"github.com/armosec/event-ingester-service/ingesters"
	ingesterutils "github.com/armosec/event-ingester-service/utils"
	postgresConnector "github.com/armosec/postgres-connector"
	postgresconnectordal "github.com/armosec/postgres-connector/dal"

	migration "github.com/armosec/postgres-connector/migration"
	"github.com/cenkalti/backoff/v4"

	"github.com/kubescape/storage/pkg/apis/softwarecomposition/v1beta1"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/adapters/backend/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/messaging"
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

	"github.com/armosec/armoapi-go/armotypes"
	"github.com/armosec/armoapi-go/identifiers"
	"github.com/armosec/armosec-infra/s3connector"
	synchronizeringester "github.com/armosec/event-ingester-service/ingesters/synchronizer_ingester"
	"github.com/armosec/event-ingester-service/synchronizer_producer"
	"github.com/armosec/postgres-connector/resourceprocessor"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	spdxv1beta1client "github.com/kubescape/storage/pkg/generated/clientset/versioned/typed/softwarecomposition/v1beta1"
	"github.com/kubescape/synchronizer/adapters/httpendpoint/v1"
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
	kubescapeNamespace = "kubescape"
	namespace          = "default"
	name               = "test"
)

type Test struct {
	ctx          context.Context
	containers   map[string]testcontainers.Container
	files        []string
	clusters     []TestKubernetesCluster
	ingesterConf ingesterutils.Configuration
	pulsarClient pulsarconnector.Client
	processor    *resourceprocessor.KubernetesResourceProcessor
	s3           *s3connector.S3Mock
	syncServers  []TestSynchronizerServer
}

type TestSynchronizerServer struct {
	ctx                       context.Context
	serverUrl                 string
	syncServer                *core.Synchronizer
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
	syncClient                *core.Synchronizer
	syncClientAdapter         *incluster.Adapter
	syncHTTPAdapter           *httpendpoint.Adapter
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
		isFreePort := true
		address := fmt.Sprintf("localhost:%s", port)
		// Trying to listen on the port - cause the port to be in use, and it takes some time for the OS to release it,
		// So we need to check if the port is available by trying to connect to it
		conn, err := net.DialTimeout("tcp", address, 1*time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		isFreePort = err != nil // port is available since we got no response
		if isFreePort && !ports.Contains(port) {
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
							Image: "quay.io/kubescape/storage:v0.0.161",
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

	kubernetesCluster := &TestKubernetesCluster{
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
	waitForStorage(t, kubernetesCluster)
	return kubernetesCluster
}

func createPulsar(t *testing.T, ctx context.Context, brokerPort, adminPort string) (testcontainers.Container, string, string) {
	pulsarC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apachepulsar/pulsar:3.0.3",
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
	require.NotNil(t, pulsarC)
	pulsarUrl, err := pulsarC.PortEndpoint(ctx, "6650", "pulsar")
	require.NoError(t, err)
	pulsarAdminUrl, err := pulsarC.PortEndpoint(ctx, "8080", "http")
	require.NoError(t, err)
	return pulsarC, pulsarUrl, pulsarAdminUrl
}

func createPostgres(t *testing.T, ctx context.Context, port string) (testcontainers.Container, string) {
	// postgres
	postgresC, _ := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
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
	require.NotNil(t, postgresC)
	pgHostPort, err := postgresC.PortEndpoint(ctx, "5432", "")
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
	// cleanup
	err = storageclient.ApplicationProfiles(namespace).Delete(context.TODO(), cluster.applicationprofile.Name, metav1.DeleteOptions{})
	require.NoError(t, err)
}

func createAndStartSynchronizerClient(t *testing.T, cluster *TestKubernetesCluster, clientConn net.Conn, syncServer *TestSynchronizerServer, httpAdapterPort string, watchDefaults bool) {
	// client side
	clientCfg, err := config.LoadConfig("../configuration/client")
	require.NoError(t, err)

	// set cluster config
	clientCfg.InCluster.Account = cluster.account
	clientCfg.InCluster.ClusterName = cluster.cluster
	clientCfg.InCluster.ExcludeNamespaces = []string{"kube-system", "kubescape"}
	clientCfg.InCluster.IncludeNamespaces = []string{}
	clientCfg.InCluster.ListPeriod = 5 * time.Second
	clientCfg.InCluster.Namespace = kubescapeNamespace
	clientCfg.InCluster.ServerUrl = syncServer.serverUrl
	if watchDefaults {
		clientCfg.InCluster.Resources = append(clientCfg.InCluster.Resources, config.Resource{
			Group:    "",
			Version:  "v1",
			Resource: "secrets",
			Strategy: "copy",
		})
		clientCfg.InCluster.Resources = append(clientCfg.InCluster.Resources, config.Resource{
			Group:    "",
			Version:  "v1",
			Resource: "serviceaccounts",
			Strategy: "copy",
		})
	}

	clientAdapter := incluster.NewInClusterAdapter(clientCfg.InCluster, dynamic.NewForConfigOrDie(cluster.clusterConfig), spdxv1beta1client.NewForConfigOrDie(cluster.clusterConfig))
	newConn := func() (net.Conn, error) {
		return clientConn, nil
	}
	clientCfg.HTTPEndpoint.ServerPort = httpAdapterPort
	httpAdapter := httpendpoint.NewHTTPEndpointAdapter(clientCfg)

	ctx, cancel := context.WithCancel(cluster.ctx)

	cluster.syncClient, err = core.NewSynchronizerClient(ctx, []adapters.Adapter{clientAdapter, httpAdapter}, clientConn, newConn)
	require.NoError(t, err)
	cluster.syncClientAdapter = clientAdapter
	cluster.syncHTTPAdapter = httpAdapter
	cluster.clientConn = clientConn
	cluster.syncClientContextCancelFn = cancel

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
	serverAdapter := backend.NewBackendAdapter(ctx, pulsarProducer, config.Backend{})
	pulsarReader.Start(ctx, serverAdapter)
	synchronizerServer, err := core.NewSynchronizerServer(ctx, []adapters.Adapter{serverAdapter}, serverConn)
	require.NoError(t, err)

	// start server
	go func() {
		_ = synchronizerServer.Start(ctx)
	}()

	return &TestSynchronizerServer{
		ctx:                       ctx,
		pulsarProducer:            pulsarProducer,
		pulsarReader:              pulsarReader,
		syncServerContextCancelFn: cancel,
		serverUrl:                 fmt.Sprintf("ws://127.0.0.1:%s/", serverPort),
		syncServer:                synchronizerServer,
		conn:                      serverConn,
	}
}

type csMock struct {
	configserviceconnector.ConfigServiceConnector
}

func (s *csMock) GetOrCreateCluster(_, _ string, _ map[string]string) (*armotypes.PortalCluster, error) {
	return &armotypes.PortalCluster{}, nil
}

func initIntegrationTest(t *testing.T) *Test {
	ctx := context.TODO()

	err := logger.L().SetLevel(helpers.DebugLevel.String())
	require.NoError(t, err)

	// create k8s cluster
	cluster1 := createK8sCluster(t, "cluster1", "b486ba4e-ffaa-4cd4-b885-b6d26cd13193")
	cluster2 := createK8sCluster(t, "cluster2", "0757d22d-a9c1-4ca3-87b6-f2236f7f5885")

	// generate some random ports
	// pulsar, pulsar-admin, postgres, sync1, sync2, sync-http1, sync-http2
	ports := randomPorts(7)

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
	time.Sleep(5 * time.Second)
	err = ingesterPgClient.Connect()
	require.NoError(t, err)
	// run migrations
	err = migration.DbMigrations(ingesterPgClient.GetClient(), migration.HotMigrationsTargetDbVersion)
	if err != nil {
		panic(fmt.Sprintf("failed to run migrations: %v", err))
	}
	pulsarClient, err := pulsarconnector.NewClient(
		pulsarconnector.WithConfig(&ingesterConf.Pulsar),
		pulsarconnector.WithRetryAttempts(20),
		pulsarconnector.WithRetryMaxDelay(3*time.Second),
	)
	require.NoError(t, err)
	ingesterProducer, err := synchronizer_producer.NewPulsarProducer(pulsarClient, ingesterConf.SynchronizerIngesterConfig.Topic)
	require.NoError(t, err)
	s3 := s3connector.NewS3Mock()
	ingesterProcessor := resourceprocessor.NewKubernetesResourceProcessor(&s3, postgresconnectordal.NewPostgresDAL(ingesterPgClient))
	ctx = context.WithValue(ctx, "resourceProcessor", ingesterProcessor)
	onFinishProducer, err := pulsarClient.NewProducer(pulsarconnector.WithProducerTopic("onFinishTopic"), pulsarconnector.WithProducerNamespace("armo", "kubescape"))
	require.NoError(t, err)
	isReady := make(chan bool)

	go synchronizeringester.ConsumeSynchronizerMessages(
		nil,
		ingesters.WithPulsarClient(pulsarClient),
		ingesters.WithIngesterConfig(ingesterConf.SynchronizerIngesterConfig),
		ingesters.WithPGConnector(ingesterPgClient),
		ingesters.WithContext(ctx),
		ingesters.WithSynchronizerProducer(ingesterProducer),
		ingesters.WithOnFinishProducer(onFinishProducer),
		ingesters.WithIsReadyChannel(isReady),
		ingesters.WithConfigServiceConnector(&csMock{}),
	)
	<-isReady

	// fake websocket
	clientConn1, serverConn1 := net.Pipe() // client1 -> server1
	clientConn2, serverConn2 := net.Pipe() // client2 -> server2

	// synchronizer servers
	synchronizerServer1 := createAndStartSynchronizerServer(t, pulsarUrl, pulsarAdminUrl, pulsarClient, serverConn1, ports[3], cluster1)
	synchronizerServer2 := createAndStartSynchronizerServer(t, pulsarUrl, pulsarAdminUrl, pulsarClient, serverConn2, ports[4], cluster2)

	// synchronizer clients
	createAndStartSynchronizerClient(t, cluster1, clientConn1, synchronizerServer1, ports[5], true)
	createAndStartSynchronizerClient(t, cluster2, clientConn2, synchronizerServer2, ports[6], true)
	time.Sleep(10 * time.Second) // important that we wait before starting the test because we might miss events if the watcher hasn't started yet
	return &Test{
		ctx: ctx,
		containers: map[string]testcontainers.Container{
			"k3s1":     cluster1.k3sC,
			"k3s2":     cluster2.k3sC,
			"postgres": postgresC,
			"pulsar":   pulsarC,
		},
		files:        []string{rdsFile.Name()},
		clusters:     []TestKubernetesCluster{*cluster1, *cluster2},
		ingesterConf: ingesterConf,
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

// grepCount returns the number of matches of a regex in a file, like grep -c regex file
func grepCount(filename, regex string) int {
	re := regexp.MustCompile(regex)
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0
	}
	matches := re.FindAllStringIndex(string(data), -1)
	return len(matches)
}

// TestSynchronizer_TC01_InCluster: Initial synchronization of a single entity
func TestSynchronizer_TC01_InCluster(t *testing.T) {
	logFile, err := os.CreateTemp("", "test-logs")
	require.NoError(t, err)
	logger.L().SetWriter(logFile)
	td := initIntegrationTest(t)
	// add applicationprofile to k8s
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Create(context.TODO(), td.clusters[0].applicationprofile, metav1.CreateOptions{})
	require.NoError(t, err)

	// check object in postgres
	objMetadata := waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	assert.Equal(t, td.clusters[0].applicationprofileDesignators, objMetadata.Designators)
	// check object in s3
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	b, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	assert.NotEmpty(t, b)
	// compare object in s3 with object in k8s
	k8sAppProfile, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	// workaround for empty TypeMeta (from k3s?)
	k8sAppProfile.TypeMeta.Kind = "ApplicationProfile"
	k8sAppProfile.TypeMeta.APIVersion = "spdx.softwarecomposition.kubescape.io/v1beta1"
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(b, &s3AppProfile)
	require.NoError(t, err)
	assert.Equal(t, k8sAppProfile, &s3AppProfile)
	// check how many times the get object message was sent
	sentGetObject := grepCount(logFile.Name(), "sent get object message")
	sentNewChecksum := grepCount(logFile.Name(), "sent new checksum message")
	// check the pulsar topic backlog is empty
	// td.ingesterConf.Pulsar.AdminUrl
	// create a new client/server pair for cluster1
	clientConn, serverConn := net.Pipe()
	// FIXME hope randomPorts(1)[0] is not the same as one of the previous ports
	newRandomPorts := randomPorts(2)
	synchronizerServer := createAndStartSynchronizerServer(t, td.ingesterConf.Pulsar.URL, td.ingesterConf.Pulsar.AdminUrl, td.pulsarClient, serverConn, newRandomPorts[0], &td.clusters[0])
	createAndStartSynchronizerClient(t, &td.clusters[0], clientConn, synchronizerServer, newRandomPorts[1], false)
	time.Sleep(20 * time.Second)

	// make sure we didn't request the same object again (as an answer to the verify object messages)
	sentGetObjectAfter := grepCount(logFile.Name(), "sent get object message")
	sentNewChecksumAfter := grepCount(logFile.Name(), "sent new checksum message")
	assert.Equal(t, sentGetObject, sentGetObjectAfter)
	assert.Greater(t, sentNewChecksumAfter, sentNewChecksum)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC01_Backend: Initial synchronization of a single entity
func TestSynchronizer_TC01_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := synchronizer_producer.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	b, err := json.Marshal(td.clusters[0].cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, b)
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
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

	objMetadata := waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	checksumInPostgresBeforeModify := objMetadata.Checksum

	time.Sleep(10 * time.Second)
	// modify applicationprofile in k8s
	k8sAppProfile, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sAppProfile.Spec.Containers[0].Name = "nginx2"
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Update(context.TODO(), k8sAppProfile, metav1.UpdateOptions{})
	require.NoError(t, err)

	// get object path from postgres, wait for checksum to change (we don't rely on resourceVersion because it's not implemented for ApplicationProfile)
	objMetadata = waitForObjectInPostgresWithDifferentChecksum(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name, checksumInPostgresBeforeModify)

	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// check object in s3
	b, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(b, &s3AppProfile)
	require.NoError(t, err)
	assert.Equal(t, "nginx2", s3AppProfile.Spec.Containers[0].Name)
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC02_Backend: Delta synchronization of a single entity
func TestSynchronizer_TC02_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := synchronizer_producer.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	b, err := json.Marshal(td.clusters[0].cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, b)
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	// modify cm via pulsar message (we don't send patches from backend)
	cm2 := td.clusters[0].cm.DeepCopy()
	cm2.Data["test"] = "test2"
	b, err = json.Marshal(cm2)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, b)
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	// check object in k8s
	k8sCm, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test2", k8sCm.Data["test"])
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC03: Conflict resolution for a single entity
// This is similar to TC02, but we modify the object simultaneously on both sides right after a connection
// failure is simulated, since versions are the same the backend modification should win
func TestSynchronizer_TC03(t *testing.T) {
	td := initIntegrationTest(t)
	pulsarProducer, err := synchronizer_producer.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	// add configmap to k8s
	_, err = td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Create(context.TODO(), td.clusters[0].cm, metav1.CreateOptions{})
	require.NoError(t, err)

	_ = waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name)

	time.Sleep(10 * time.Second)

	// kill network connection on client side
	dead, _ := net.Pipe()
	err = dead.Close()
	require.NoError(t, err)
	*td.clusters[0].syncClient.Conn = dead

	// modify configmap in k8s
	k8sCm, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sCm.Data["test"] = "cluster"
	_, err = td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Update(context.TODO(), k8sCm, metav1.UpdateOptions{})
	require.NoError(t, err)

	// modify configmap in backend
	cm2 := td.clusters[0].cm.DeepCopy()
	cm2.Data["test"] = "test2"
	b, err := json.Marshal(cm2)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, b)
	require.NoError(t, err)
	time.Sleep(10 * time.Second)

	// check object in k8s
	k8sCm2, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test2", k8sCm2.Data["test"])
	// tear down
	tearDown(td)
}

// TestSynchronizer_TC04_InCluster: Deletion of a single entity
// we use configmap here because our CRDs no longer propagates deletion because of the periodic listings
// (we now rely on the reconciliation batch to detect deletions)
func TestSynchronizer_TC04_InCluster(t *testing.T) {
	td := initIntegrationTest(t)
	// add applicationprofile to k8s
	_, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Create(context.TODO(), td.clusters[0].cm, metav1.CreateOptions{})
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	// check object in postgres
	objMetadata := waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name)
	assert.NotNil(t, objMetadata)
	// delete applicationprofile from k8s
	err = td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	require.NoError(t, err)

	// check object not in postgres
	waitForObjectToBeDeletedInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name)

	// tear down
	tearDown(td)
}

// TestSynchronizer_TC04_Backend: Deletion of a single entity
func TestSynchronizer_TC04_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add cm to backend via pulsar message
	pulsarProducer, err := synchronizer_producer.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)
	b, err := json.Marshal(td.clusters[0].cm)
	require.NoError(t, err)
	err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0, b)
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	// check object in k8s
	k8sCm, err := td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "test", k8sCm.Data["test"])
	// delete cm via pulsar message
	err = pulsarProducer.SendDeleteObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, "/v1/configmaps", namespace, name, "", 0)
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	// check object not in k8s
	_, err = td.clusters[0].k8sclient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	assert.Error(t, err)
	// tear down
	tearDown(td)
}

func getPodNamesInNamespace(k8sClient kubernetes.Interface, namespace string) []string {
	var podNames []string
	pods, err := k8sClient.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return podNames
	}

	for _, pod := range pods.Items {
		podNames = append(podNames, pod.Name)
	}
	return podNames
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
	time.Sleep(20 * time.Second)
	// check objects in postgres
	for _, kind := range []string{"spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", "apps/v1/deployments", "apps/v1/statefulsets"} {
		_ = waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, kind, namespace, name)
	}
	// check that pods in a non-kubescape namespace were not synchronized
	for _, podName := range getPodNamesInNamespace(td.clusters[0].k8sclient, namespace) {
		_, objFound, _ := td.processor.GetObjectFromPostgres(td.clusters[0].account, td.clusters[0].cluster, "/v1/pods", namespace, podName)
		assert.False(t, objFound)
	}

	// create a deployment in the kubescape namespace
	_, err = td.clusters[0].k8sclient.AppsV1().Deployments(kubescapeNamespace).Create(context.TODO(), td.clusters[0].deploy, metav1.CreateOptions{})
	require.NoError(t, err)
	_ = waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "apps/v1/deployments", kubescapeNamespace, name)

	// check that pods in the kubescape namespace are synchronized
	for _, podName := range getPodNamesInNamespace(td.clusters[0].k8sclient, kubescapeNamespace) {
		_ = waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "/v1/pods", kubescapeNamespace, podName)
	}

	// tear down
	tearDown(td)
}

// TestSynchronizer_TC05_Backend: Synchronizing entities with different types
// this test also checks that the synchronizer does not create objects in clusters that are not the source of the message
func TestSynchronizer_TC05_Backend(t *testing.T) {
	td := initIntegrationTest(t)
	// add objects to backend via pulsar message
	pulsarProducer, err := synchronizer_producer.NewPulsarProducer(td.pulsarClient, "synchronizer")
	require.NoError(t, err)

	// cluster 1
	objsC1 := map[string]any{
		"/v1/configmaps": td.clusters[0].cm,
		"/v1/secrets":    td.clusters[0].secret,
	}
	for kind, obj := range objsC1 {
		b, err := json.Marshal(obj)
		require.NoError(t, err)
		err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[0].account, td.clusters[0].cluster, kind, namespace, name, "", 0, b)
		require.NoError(t, err)
	}

	// cluster 2
	objsC2 := map[string]any{
		"/v1/serviceaccounts": td.clusters[1].sa,
	}
	for kind, obj := range objsC2 {
		b, err := json.Marshal(obj)
		require.NoError(t, err)
		err = pulsarProducer.SendPutObjectMessage(td.ctx, td.clusters[1].account, td.clusters[1].cluster, kind, namespace, name, "", 0, b)
		require.NoError(t, err)
	}

	time.Sleep(10 * time.Second)

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

	objMetadata := waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	checksumInPostgresBeforeModify := objMetadata.Checksum

	time.Sleep(20 * time.Second)
	// prepare to alter shadow object in client
	appClient, err := td.clusters[0].syncClientAdapter.GetClient(domain.KindName{Kind: domain.KindFromString(context.TODO(), "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles")})
	require.NoError(t, err)
	// modify applicationprofile in k8s
	k8sAppProfile, err := td.clusters[0].storageclient.ApplicationProfiles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	k8sAppProfile.Spec.Containers[0].Name = "nginx2"
	// alter shadow object in client before updating k8s, the patch won't include the image change
	appClient.(*incluster.Client).ShadowObjects["spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles/default/test"], err = json.Marshal(k8sAppProfile)
	require.NoError(t, err)
	_, err = td.clusters[0].storageclient.ApplicationProfiles(namespace).Update(context.TODO(), k8sAppProfile, metav1.UpdateOptions{})
	require.NoError(t, err)

	// get object path from postgres, wait for checksum to change (we don't rely on resourceVersion because it's not implemented for ApplicationProfile)
	objMetadata = waitForObjectInPostgresWithDifferentChecksum(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name, checksumInPostgresBeforeModify)
	var objPath s3connector.S3ObjectPath
	err = json.Unmarshal([]byte(objMetadata.ResourceObjectRef), &objPath)
	require.NoError(t, err)
	// check object in s3
	b, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(b, &s3AppProfile)
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

	objMetadata := waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)

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
	time.Sleep(10 * time.Second)
	// check object in s3
	b, err := td.processor.GetObjectFromS3(objPath)
	assert.NoError(t, err)
	var s3AppProfile v1beta1.ApplicationProfile
	err = json.Unmarshal(b, &s3AppProfile)
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
	time.Sleep(20 * time.Second)
	// check object in postgres
	objMetadata := waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	assert.NotNil(t, objMetadata)
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
	time.Sleep(10 * time.Second)
	// restart pulsar
	err = td.containers["pulsar"].Start(td.ctx)
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
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
	time.Sleep(10 * time.Second)
	// restart postgres
	err = td.containers["postgres"].Start(td.ctx)

	require.NoError(t, err)
	time.Sleep(10 * time.Second)
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
	require.NoError(t, err)
	time.Sleep(10 * time.Second)
	// re-enable S3Mock
	td.s3.SetReturnError(false)

	// check object in postgres
	objMetadata := waitForObjectInPostgres(t, td, td.clusters[0].account, td.clusters[0].cluster, "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles", namespace, name)
	require.NotNil(t, objMetadata)

	// tear down
	tearDown(td)
}

// TestSynchronizer_TC12: Reconciliation flow
// we have one application profile in k8s cluster, then, we delete this resource from postgres and create a dummy resource to postgres,
// a reconciliation message is produced by the ingester with the two resources (one that exists in k8s with a different resource version and one that does not exist in k8s)
// we expect to see the resource that exists in k8s to be back in postgres and the resource that does not exist in k8s to be deleted from postgres
func TestSynchronizer_TC12(t *testing.T) {
	td := initIntegrationTest(t)

	cluster := td.clusters[0]
	clusterName := cluster.cluster
	account := cluster.account
	kind := "spdx.softwarecomposition.kubescape.io/v1beta1/applicationprofiles"

	// add applicationprofile to k8s
	createdAppProfileObj, err := cluster.storageclient.ApplicationProfiles(namespace).Create(context.TODO(), cluster.applicationprofile, metav1.CreateOptions{})

	// wait for the object to be created in postgres
	_ = waitForObjectInPostgres(t, td, account, clusterName, kind, namespace, name)

	// create a new dummy object directly in postgres (which does not exist in k8s) and confirm it is there
	toBeDeletedName := name + "test"
	b, _ := json.Marshal(createdAppProfileObj)
	_, err = td.processor.Store(context.Background(), account, clusterName, kind, namespace, toBeDeletedName, b, nil)
	assert.NoError(t, err)
	_, objFound, err := td.processor.GetObjectFromPostgres(account, clusterName, kind, namespace, toBeDeletedName)
	assert.NoError(t, err)
	assert.True(t, objFound)

	// delete the resource that exists in k8s from postgres and confirm it is deleted
	_, err = td.processor.Delete(account, clusterName, kind, namespace, name)
	assert.NoError(t, err)
	_, objFound, err = td.processor.GetObjectFromPostgres(account, clusterName, kind, namespace, name)
	assert.Error(t, err)
	assert.False(t, objFound)

	// produce a reconciliation message with the two resources from postgres
	msg := messaging.ReconciliationRequestMessage{
		Cluster:         clusterName,
		Account:         account,
		Depth:           1,
		MsgId:           "test-msg-id",
		ServerInitiated: true,
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err)
	err = td.syncServers[0].pulsarProducer.ProduceMessage(td.syncServers[0].ctx,
		domain.ClientIdentifier{
			Cluster: clusterName,
			Account: account,
		}, messaging.MsgPropEventValueReconciliationRequestMessage, data)
	assert.NoError(t, err)

	time.Sleep(30 * time.Second)

	// check that object is back in postgres
	_ = waitForObjectInPostgres(t, td, account, clusterName, kind, namespace, name)
	// check that object is deleted from postgres
	waitForObjectToBeDeletedInPostgres(t, td, account, clusterName, kind, namespace, toBeDeletedName)

	// tear down
	tearDown(td)
}

// TestSynchronizer_TC13_HTTPEndpoint: Test the HTTP endpoint of the synchronizer
// Try to send an alert and see the message in Pulsar (we have example of producing)
func TestSynchronizer_TC13_HTTPEndpoint(t *testing.T) {
	td := initIntegrationTest(t)
	defer tearDown(td)

	alertBytes, err := os.ReadFile("../tests/mockdata/alert.json")
	require.NoError(t, err)
	// http.DefaultClient.Timeout = 10 * time.Second
	for httpAdapterIdx := range td.clusters {
		syncHTTPAdapterPort := td.clusters[httpAdapterIdx].syncHTTPAdapter.GetConfig().HTTPEndpoint.ServerPort
		// check that the endpoint is up
		for i := 0; i < 10; i++ {
			resp, err := http.Get(fmt.Sprintf("http://localhost:%s/readyz", syncHTTPAdapterPort))
			if err == nil && resp.StatusCode == http.StatusOK {
				break
			}
			time.Sleep(1 * time.Second)
		}

		resp, err := http.Post(fmt.Sprintf("http://localhost:%s/apis/v1/test-ks/v1/alerts", syncHTTPAdapterPort), "application/json", bytes.NewReader(alertBytes))
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
	}

	// check the 2 messages are in pulsar
	// open a reader:
	reader, err := td.pulsarClient.CreateReader(pulsar.ReaderOptions{
		Topic:          "persistent://armo/kubescape/synchronizer",
		StartMessageID: pulsar.EarliestMessageID(),
	})
	require.NoError(t, err)
	defer reader.Close()
	// read the messages
	var messages []pulsar.Message
	for len(messages) < 2 {
		ctx, cancelFunc := context.WithTimeout(td.ctx, 10*time.Second)
		defer cancelFunc()
		msg, err := reader.Next(ctx)
		require.NoError(t, err)
		// compare the messages payload to alertBytes
		putMsg := messaging.PutObjectMessage{}
		err = json.Unmarshal(msg.Payload(), &putMsg)
		require.NoError(t, err)
		if putMsg.Object == nil || putMsg.Kind != "test-ks/v1/alerts" {
			continue
		}
		// compare gvr
		assert.Equal(t, putMsg.Kind, fmt.Sprintf("%s/%s/%s", msg.Properties()["group"], msg.Properties()["version"], msg.Properties()["resource"]))

		assert.Equal(t, alertBytes, putMsg.Object)
		assert.Equal(t, putMsg.Account, msg.Properties()["account"])
		messages = append(messages, msg)
	}

	assert.Equal(t, "cluster1", messages[0].Properties()["cluster"])
	assert.Equal(t, "b486ba4e-ffaa-4cd4-b885-b6d26cd13193", messages[0].Properties()["account"])
	assert.Equal(t, "cluster2", messages[1].Properties()["cluster"])
	assert.Equal(t, "0757d22d-a9c1-4ca3-87b6-f2236f7f5885", messages[1].Properties()["account"])
}

// waitForObjectInPostgres waits for the object to be present in postgres and returns the object metadata
func waitForObjectInPostgres(t *testing.T, td *Test, account, clusterName, kind, namespace, name string) *armotypes.KubernetesObject {
	return waitForObjectInPostgresWithDifferentChecksum(t, td, account, clusterName, kind, namespace, name, "")
}

// waitForObjectInPostgres waits for the object to be present in postgres and returns the object metadata
func waitForObjectToBeDeletedInPostgres(t *testing.T, td *Test, account, clusterName, kind, namespace, name string) {
	var objFound bool
	var err error
	for i := 0; i < 10; i++ {
		_, objFound, err = td.processor.GetObjectFromPostgres(account, clusterName, kind, namespace, name)
		if err != nil && !objFound {
			return
		}
		time.Sleep(10 * time.Second)
	}
	require.False(t, objFound, "Object found in postgres")
	require.Error(t, err)
}

func waitForObjectInPostgresWithDifferentChecksum(t *testing.T, td *Test, account, clusterName, kind, namespace, name, checksum string) *armotypes.KubernetesObject {
	var objFound bool
	var objMetadata *armotypes.KubernetesObject
	var err error
	for i := 0; i < 10; i++ {
		objMetadata, objFound, err = td.processor.GetObjectFromPostgres(account, clusterName, kind, namespace, name)
		if objFound {
			if checksum == "" {
				break
			}

			if objMetadata != nil && objMetadata.Checksum != checksum {
				break
			}
		}
		time.Sleep(10 * time.Second)
	}
	require.True(t, objFound, "Object not found in postgres")
	require.NoError(t, err)
	require.NotNil(t, objMetadata)
	if checksum != "" {
		//goland:noinspection GoDfaErrorMayBeNotNil,GoDfaNilDereference
		require.NotEqual(t, checksum, objMetadata.Checksum, "expected object with different checksum in postgres")
	}

	return objMetadata
}
