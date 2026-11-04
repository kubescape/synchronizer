package httpendpoint

import (
	"net/http"
	"strings"

	"github.com/kubescape/storage/pkg/apiauth"
)

// Route maps an ingest request to the RBAC attributes checked by apiauth.
// Probes are open. /apis/v1/<group>/<version>/<resource> is checked as
// "create" on <group>/<resource> whatever the HTTP method. Any other path is
// protected as an unknown resource so RBAC denies it instead of passing it.
func Route(r *http.Request) (apiauth.Attributes, bool) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		return apiauth.Attributes{}, false
	}
	s := strings.Split(strings.ToLower(r.URL.Path), "/")
	if len(s) == 6 && s[0] == "" && s[1] == "apis" && s[2] == "v1" && s[3] != "" && s[4] != "" && s[5] != "" {
		return apiauth.Attributes{Group: s[3], Resource: s[5], Verb: "create"}, true
	}
	return apiauth.Attributes{Group: "kubescape.io", Resource: "unknown", Verb: "create"}, true
}
