package messaging

import (
	"strconv"

	"github.com/kubescape/synchronizer/domain"
)

func KindNameToCustomProperties(kind domain.KindName) map[string]string {
	return map[string]string{
		MsgPropResourceKindGroup:    kind.Kind.Group,
		MsgPropResourceKindVersion:  kind.Kind.Version,
		MsgPropResourceKindResource: kind.Kind.Resource,
		MsgPropResourceName:         kind.Name,
		MsgPropResourceNamespace:    kind.Namespace,
		MsgPropResourceVersion:      strconv.Itoa(kind.ResourceVersion),
	}
}
