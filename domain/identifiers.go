package domain

import (
	"strconv"
	"strings"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type KindName struct {
	Kind            *Kind
	Name            string
	Namespace       string
	ResourceVersion int
}

func (c KindName) ToCustomProperties() map[string]string {
	return map[string]string{
		"group":           c.Kind.Group,
		"version":         c.Kind.Version,
		"resource":        c.Kind.Resource,
		"name":            c.Name,
		"namespace":       c.Namespace,
		"resourceVersion": strconv.Itoa(c.ResourceVersion),
	}
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

func FromUnstructured(u *unstructured.Unstructured) KindName {
	// TODO: tests
	if u == nil {
		logger.L().Error("Unable to convert nil unstructured to KindName")
		return KindName{}
	}
	apiVerSlices := strings.Split(strings.ToLower(u.GetAPIVersion()), "/")
	if len(apiVerSlices) == 0 {
		logger.L().Error("Unable to convert apiVersion to KindName", helpers.String("apiVersion", u.GetAPIVersion()))
		return KindName{}
	}
	if len(apiVerSlices) == 1 {
		apiVerSlices = append(apiVerSlices, "v1")
	}

	return KindName{
		Kind: &Kind{
			Resource: strings.ToLower(u.GetKind()),
			Group:    apiVerSlices[0],
			Version:  apiVerSlices[1],
		},
		Name:            strings.ToLower(u.GetName()),
		Namespace:       strings.ToLower(u.GetNamespace()),
		ResourceVersion: ToResourceVersion(u.GetResourceVersion()),
	}

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
