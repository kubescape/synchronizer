package domain

import "strings"

type ClusterKindName struct {
	Account string
	Cluster string
	Kind    *Kind
	Name    string
}

func (c ClusterKindName) String() string {
	var kind string
	if c.Kind == nil {
		kind = ""
	} else {
		kind = c.Kind.String()
	}
	return strings.Join([]string{c.Account, c.Cluster, kind, c.Name}, "/")
}

func (c ClusterKindName) IdentifierKey() string {
	return strings.Join([]string{c.Account, c.Cluster}, "/")
}
