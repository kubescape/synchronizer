package domain

import "strings"

type ClusterKindName struct {
	Cluster string
	Kind    *Kind
	Name    string
}

func (c ClusterKindName) String() string {
	return strings.Join([]string{c.Cluster, c.Kind.String(), c.Name}, "/")
}
