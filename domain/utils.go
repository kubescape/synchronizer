package domain

import "strings"

// Kind returns group/version/resource as a string.
func (k Kind) String() string {
	return strings.Join([]string{k.Group, k.Version, k.Resource}, "/")
}
