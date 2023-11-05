package domain

import (
	"strings"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
)

// Kind returns group/version/resource as a string.
func (k Kind) String() string {
	return strings.Join([]string{k.Group, k.Version, k.Resource}, "/")
}

func KindFromString(kind string) *Kind {
	parts := strings.Split(kind, "/")
	if len(parts) != 3 {
		logger.L().Error("failed creating kind from string", helpers.String("kind", kind))
		return nil
	}
	return &Kind{
		Group:    parts[0],
		Version:  parts[1],
		Resource: parts[2],
	}
}
