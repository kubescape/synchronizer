package config

import (
	"strings"

	"github.com/kubescape/synchronizer/domain"
	"github.com/spf13/viper"
)

type Config struct {
	Backend   string     `mapstructure:"backend"`
	Cluster   string     `mapstructure:"cluster"`
	Resources []Resource `mapstructure:"resources"`
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
