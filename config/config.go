package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/armosec/utils-k8s-go/armometadata"
	"github.com/kubescape/backend/pkg/servicediscovery"
	"github.com/kubescape/backend/pkg/servicediscovery/schema"
	v3 "github.com/kubescape/backend/pkg/servicediscovery/v3"
	pulsarconfig "github.com/kubescape/messaging/pulsar/config"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/domain"
	"github.com/spf13/viper"
)

type Config struct {
	Backend      Backend      `mapstructure:"backend"`
	InCluster    InCluster    `mapstructure:"inCluster"`
	HTTPEndpoint HTTPEndpoint `mapstructure:"httpEndpoint"`
}

type Backend struct {
	Port                 int                         `mapstructure:"port"`
	AuthenticationServer *AuthenticationServerConfig `mapstructure:"authenticationServer"`
	Subscription         string                      `mapstructure:"subscription"`
	PulsarConfig         *pulsarconfig.PulsarConfig  `mapstructure:"pulsarConfig"`
	ProducerTopic        pulsarconnector.TopicName   `mapstructure:"producerTopic"`
	ConsumerTopic        pulsarconnector.TopicName   `mapstructure:"consumerTopic"`
	ConsumerWorkers      int                         `mapstructure:"consumerWorkers"`
	Prometheus           *PrometheusConfig           `mapstructure:"prometheusConfig"`
	ReconciliationTask   *ReconciliationTaskConfig   `mapstructure:"reconciliationTaskConfig"`
	KeepAliveTask        *KeepAliveTaskConfig        `mapstructure:"keepAliveTaskConfig"`
	SkipAlertsFrom       []string                    `mapstructure:"skipAlertsFrom"`
	FeaturesProvider     *FeaturesProviderConfig     `mapstructure:"featuresProvider"`
	MessageQueue         *MessageQueueConfig         `mapstructure:"messageQueue"`
}

// MessageQueueConfig selects the message queue backend. When Type is set it
// takes precedence over the legacy top-level PulsarConfig; when it is absent the
// server falls back to PulsarConfig (see adapters/backend/v1/factory.go).
type MessageQueueConfig struct {
	Type        string       `mapstructure:"type"` // "kafka"
	KafkaConfig *KafkaConfig `mapstructure:"kafkaConfig"`
}

type KafkaConfig struct {
	BootstrapServers []string `mapstructure:"bootstrapServers"`
	ProducerTopic    string   `mapstructure:"producerTopic"` // server produces here; carries cluster -> backend traffic (.out), consumed by the event ingester
	ConsumerTopic    string   `mapstructure:"consumerTopic"` // server consumes here; carries backend -> cluster traffic (.in), produced by backend services
	// false by default, since brokers usually auto-create on first produce. set it where
	// auto-creation is off, so a missing topic fails startup instead of silently never syncing
	RequireTopicsExist bool   `mapstructure:"requireTopicsExist"`
	GroupIDPrefix      string `mapstructure:"groupIdPrefix"`
	CompressionType    string `mapstructure:"compressionType"` // default "zstd"
	MaxMessageBytes    int    `mapstructure:"maxMessageBytes"` // default 67108864 (64 MB)
	// securityProtocol is the authoritative selector: the SASL_ prefix enables SASL,
	// the _SSL suffix enables TLS, and an empty value is treated as PLAINTEXT.
	SecurityProtocol string `mapstructure:"securityProtocol"` // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	SASLMechanism    string `mapstructure:"saslMechanism"`    // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUsername     string `mapstructure:"saslUsername"`
	SASLPassword     string `mapstructure:"saslPassword"`  // rendered from a Secret by Helm
	TLSCaCertPath    string `mapstructure:"tlsCaCertPath"` // optional custom CA for the _SSL protocols
}

type InCluster struct {
	ServerUrl         string     `mapstructure:"serverUrl"`
	Namespace         string     `mapstructure:"namespace"`
	ClusterName       string     `mapstructure:"clusterName"`
	ExcludeNamespaces []string   `mapstructure:"excludeNamespaces"`
	IncludeNamespaces []string   `mapstructure:"includeNamespaces"`
	Account           string     `mapstructure:"account"`
	AccessKey         string     `mapstructure:"accessKey"`
	Resources         []Resource `mapstructure:"resources"`
}

type HTTPEndpoint struct {
	ServerPort string     `mapstructure:"serverPort"`
	Resources  []Resource `mapstructure:"resources"`
}

type Resource struct {
	Group    string          `mapstructure:"group"`
	Version  string          `mapstructure:"version"`
	Resource string          `mapstructure:"resource"`
	Strategy domain.Strategy `mapstructure:"strategy"`
}

type AuthenticationServerConfig struct {
	Url                       string            `mapstructure:"url"`
	HeaderToQueryParamMapping map[string]string `mapstructure:"headerToQueryParamMapping"`
	HeaderToHeaderMapping     map[string]string `mapstructure:"headerToHeaderMapping"`
}

type PrometheusConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

type ReconciliationTaskConfig struct {
	CronSchedule                  string `mapstructure:"cronSchedule"` // when this is set, taskIntervalSeconds is ignored
	TaskIntervalSeconds           int    `mapstructure:"taskIntervalSeconds"`
	IntervalFromConnectionSeconds int    `mapstructure:"intervalFromConnectionSeconds"`
	SendBatch                     bool   `mapstructure:"sendBatch"` // if true, a single message will be sent for all connected clients
}

type KeepAliveTaskConfig struct {
	TaskIntervalSeconds int `mapstructure:"taskIntervalSeconds"`
}

type FeaturesProviderConfig struct {
	FeatureFlagName        string `mapstructure:"featureFlagName"`
	RefreshIntervalSeconds int    `mapstructure:"refreshIntervalSeconds"`
}

// Kind returns group/version/resource as a string.
func (r Resource) String() string {
	return strings.Join([]string{r.Group, r.Version, r.Resource}, "/")
}

// LoadConfig reads configuration from file or environment variables.
func LoadConfig(path string) (Config, error) {
	v := viper.New() // singleton prevents running tests in parallel
	if configPathFromEnv := os.Getenv("CONFIG"); configPathFromEnv != "" {
		v.AddConfigPath(configPathFromEnv)
	}
	v.AddConfigPath(path)
	v.SetConfigName("config")
	v.SetConfigType("json")

	v.AutomaticEnv()

	err := v.ReadInConfig()
	if err != nil {
		return Config{}, err
	}

	var config Config
	err = v.Unmarshal(&config)
	return config, err
}

func LoadClusterConfig() (armometadata.ClusterConfig, error) {
	pathAndFileName, present := os.LookupEnv("CLUSTER_CONFIG")
	if !present {
		pathAndFileName = "/etc/config/clusterData.json"
	}

	clusterConfig, err := armometadata.LoadConfig(pathAndFileName)
	if err != nil {
		return armometadata.ClusterConfig{}, err
	}

	return *clusterConfig, err
}

func LoadServiceURLs(apiURL string) (schema.IBackendServices, error) {
	client, err := v3.NewServiceDiscoveryClientV3(apiURL)
	if err != nil {
		return nil, err
	}
	return servicediscovery.GetServices(client)
}

func (c *InCluster) ValidateConfig() error {
	if c.AccessKey == "" {
		return fmt.Errorf("access key is missing")
	}
	if c.Account == "" {
		return fmt.Errorf("account is missing")
	}
	if c.ClusterName == "" {
		return fmt.Errorf("cluster name is missing")
	}
	if c.ServerUrl == "" {
		return fmt.Errorf("server url is missing")
	}
	if len(c.Resources) == 0 {
		return fmt.Errorf("resources are missing")
	}
	return nil
}

func (c *HTTPEndpoint) ValidateConfig() error {
	if c.ServerPort == "" {
		return fmt.Errorf("server port is missing")
	}
	if len(c.Resources) == 0 {
		return fmt.Errorf("resources are missing")
	}
	return nil
}
