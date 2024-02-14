package domain

import (
	"strconv"
	"strings"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
)

type KindName struct {
	Kind            *Kind
	Name            string
	Namespace       string
	ResourceVersion int
}

func (c KindName) String() string {
	var kind string
	if c.Kind == nil {
		kind = ""
	} else {
		kind = c.Kind.String()
	}
	return strings.Join([]string{kind, c.Namespace, c.Name}, "/")
}

func ToResourceVersion(version string) int {
	if v, err := strconv.Atoi(version); err == nil {
		return v
	}

	logger.L().Error("Unable to convert version to int", helpers.String("version", version))
	return 0
}

type ClientIdentifier struct {
	Account        string
	Cluster        string
	ConnectionId   string
	ConnectionTime time.Time
	HelmVersion    string
	Version        string
}

func (c ClientIdentifier) String() string {
	return strings.Join([]string{c.Account, c.Cluster}, "/")
}

func (c ClientIdentifier) ConnectionString() string {
	return strings.Join([]string{c.Account, c.Cluster, c.ConnectionId}, "/")
}
