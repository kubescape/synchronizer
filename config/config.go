package config

import (
	"os"
	"strings"

	"github.com/armosec/utils-k8s-go/armometadata"
	"github.com/kubescape/backend/pkg/servicediscovery"
	"github.com/kubescape/backend/pkg/servicediscovery/schema"
	v2 "github.com/kubescape/backend/pkg/servicediscovery/v2"
	"github.com/kubescape/go-logger"
	pulsarconfig "github.com/kubescape/messaging/pulsar/config"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/domain"
	"github.com/spf13/viper"
)

type Config struct {
	Backend   Backend   `mapstructure:"backend"`
	InCluster InCluster `mapstructure:"inCluster"`
}

type Backend struct {
	AuthenticationServer *AuthenticationServerConfig `mapstructure:"authenticationServer"`
	Subscription         string                      `mapstructure:"subscription"`
	PulsarConfig         *pulsarconfig.PulsarConfig  `mapstructure:"pulsarConfig"`
	Topic                pulsarconnector.TopicName   `mapstructure:"topic"`
}

type InCluster struct {
	ServerUrl   string     `mapstructure:"serverUrl"`
	ClusterName string     `mapstructure:"clusterName"`
	Account     string     `mapstructure:"account"`
	AccessKey   string     `mapstructure:"accessKey"`
	Resources   []Resource `mapstructure:"resources"`
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

func LoadServiceURLs(filePath string) (schema.IBackendServices, error) {
	pathAndFileName, present := os.LookupEnv("SERVICES")
	if !present {
		pathAndFileName = filePath
	}
	return servicediscovery.GetServices(
		v2.NewServiceDiscoveryFileV2(pathAndFileName),
	)
}

func (c *InCluster) ValidateConfig() {
	if c.AccessKey == "" {
		logger.L().Fatal("access key is missing")
	}
	if c.Account == "" {
		logger.L().Fatal("account is missing")
	}
	if c.ClusterName == "" {
		logger.L().Fatal("cluster name is missing")
	}
	if c.ServerUrl == "" {
		logger.L().Fatal("server url is missing")
	}
	if len(c.Resources) == 0 {
		logger.L().Fatal("resources are missing")
	}
}
