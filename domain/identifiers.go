package domain

import (
	"strings"
	"time"
)

type KindName struct {
	Kind      *Kind
	Name      string
	Namespace string
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

type ClientIdentifier struct {
	Account        string
	Cluster        string
	ConnectionId   string
	ConnectionTime time.Time
}

func (c ClientIdentifier) String() string {
	return strings.Join([]string{c.Account, c.Cluster}, "/")
}

func (c ClientIdentifier) ConnectionString() string {
	return strings.Join([]string{c.Account, c.Cluster, c.ConnectionId}, "/")
}
