package config

import (
	"os"
	"strings"

	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/domain"
	"github.com/spf13/viper"
)

type Config struct {
	Backend   Backend    `mapstructure:"backend"`
	InCluster InCluster  `mapstructure:"inCluster"`
	Resources []Resource `mapstructure:"resources"`
}

type Backend struct {
	Subscription string                    `mapstructure:"subscription"`
	SyncTopic    pulsarconnector.TopicName `mapstructure:"syncTopic"`
	Topic        pulsarconnector.TopicName `mapstructure:"topic"`
}

type InCluster struct {
	BackendUrl  string `mapstructure:"backendUrl"`
	ClusterName string `mapstructure:"clusterName"`
	Account     string `mapstructure:"account"`
	AccessKey   string `mapstructure:"accessKey"`
}

type Resource struct {
	Group    string          `mapstructure:"group"`
	Version  string          `mapstructure:"version"`
	Resource string          `mapstructure:"resource"`
	Strategy domain.Strategy `mapstructure:"strategy"`
}

// Kind returns group/version/resource as a string.
func (r Resource) String() string {
	return strings.Join([]string{r.Group, r.Version, r.Resource}, "/")
}

// LoadConfig reads configuration from file or environment variables.
func LoadConfig(path string) (Config, error) {
	if configPathFromEnv := os.Getenv("CONFIG_PATH"); configPathFromEnv != "" {
		viper.AddConfigPath(configPathFromEnv)
	}
	viper.AddConfigPath(path)
	viper.SetConfigName("config")
	viper.SetConfigType("json")

	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		return Config{}, err
	}

	var config Config
	err = viper.Unmarshal(&config)
	return config, err
}
