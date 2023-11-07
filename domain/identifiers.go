package domain

import "strings"

type KindName struct {
	Kind *Kind
	Name string
}

func (c KindName) String() string {
	var kind string
	if c.Kind == nil {
		kind = ""
	} else {
		kind = c.Kind.String()
	}
	return strings.Join([]string{kind, c.Name}, "/")
}

type ClientIdentifier struct {
	Account string
	Cluster string
}

func (c ClientIdentifier) String() string {
	return strings.Join([]string{c.Account, c.Cluster}, "/")
}
