package messaging

import "time"

const (
	MsgPropTimestamp = "timestamp"
	// MsgPropCluster is the property name for the cluster name
	MsgPropCluster = "cluster"
	// MsgPropAccount is the property name for the account name
	MsgPropAccount = "account"
	// MsgPropEvent is the property name for the event type
	MsgPropEvent = "event"
	// MsgPropResourceKindGroup is the property name for the API Group of the resource
	MsgPropResourceKindGroup = "group"
	// MsgPropResourceKindVersion is the property name for the API Version of the resource
	MsgPropResourceKindVersion = "version"
	// MsgPropResourceKindResource is the property name for the API Resource of the resource
	MsgPropResourceKindResource = "resource"
	// MsgPropResourceName is the property name for the name of the resource
	MsgPropResourceName = "name"
	// MsgPropResourceNamespace is the property name for the namespace of the resource
	MsgPropResourceNamespace = "namespace"
	// MsgPropResourceVersion is the property name for the resource version of the resource
	MsgPropResourceVersion = "resourceVersion"

	MsgPropEventValueGetObjectMessage             = "GetObject"
	MsgPropEventValuePatchObjectMessage           = "PatchObject"
	MsgPropEventValueVerifyObjectMessage          = "VerifyObject"
	MsgPropEventValueDeleteObjectMessage          = "DeleteObject"
	MsgPropEventValuePutObjectMessage             = "PutObject"
	MsgPropEventValueServerConnectedMessage       = "ServerConnected"
	MsgPropEventValueReconciliationRequestMessage = "ReconciliationRequest"
	MsgPropEventValueConnectedClientsMessage      = "ConnectedClients"
)

type DeleteObjectMessage struct {
	Cluster         string `json:"cluster"`
	Account         string `json:"account"`
	Depth           int    `json:"depth"`
	Kind            string `json:"kind"`
	MsgId           string `json:"msgId"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion int    `json:"resourceVersion"`
}

type GetObjectMessage struct {
	BaseObject      []byte `json:"baseObject"`
	Cluster         string `json:"cluster"`
	Account         string `json:"account"`
	Depth           int    `json:"depth"`
	Kind            string `json:"kind"`
	MsgId           string `json:"msgId"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion int    `json:"resourceVersion"`
}

type NewChecksumMessage struct {
	Checksum        string `json:"checksum"`
	Cluster         string `json:"cluster"`
	Account         string `json:"account"`
	Depth           int    `json:"depth"`
	Kind            string `json:"kind"`
	MsgId           string `json:"msgId"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion int    `json:"resourceVersion"`
}

type NewObjectMessage struct {
	Cluster         string `json:"cluster"`
	Account         string `json:"account"`
	Depth           int    `json:"depth"`
	Kind            string `json:"kind"`
	MsgId           string `json:"msgId"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	Object          []byte `json:"patch"`
	ResourceVersion int    `json:"resourceVersion"`
}

type PatchObjectMessage struct {
	Checksum        string `json:"checksum"`
	Cluster         string `json:"cluster"`
	Account         string `json:"account"`
	Depth           int    `json:"depth"`
	Kind            string `json:"kind"`
	MsgId           string `json:"msgId"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	Patch           []byte `json:"patch"`
	ResourceVersion int    `json:"resourceVersion"`
}

type PutObjectMessage struct {
	Cluster         string `json:"cluster"`
	Account         string `json:"account"`
	Depth           int    `json:"depth"`
	Kind            string `json:"kind"`
	MsgId           string `json:"msgId"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	Object          []byte `json:"patch"`
	ResourceVersion int    `json:"resourceVersion"`
}

type VerifyObjectMessage struct {
	Checksum        string `json:"checksum"`
	Cluster         string `json:"cluster"`
	Account         string `json:"account"`
	Depth           int    `json:"depth"`
	Kind            string `json:"kind"`
	MsgId           string `json:"msgId"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion int    `json:"resourceVersion"`
}

type ServerConnectedMessage struct {
	Cluster string `json:"cluster"`
	Account string `json:"account"`
	Depth   int    `json:"depth"`
	MsgId   string `json:"msgId"`
}

type ReconciliationRequestMessage struct {
	Cluster         string                                   `json:"cluster"`
	Account         string                                   `json:"account"`
	Depth           int                                      `json:"depth"`
	MsgId           string                                   `json:"msgId"`
	ServerInitiated bool                                     `json:"serverInitiated"`
	KindToObjects   map[string][]ReconciliationRequestObject `json:"kindToObject"`
}

type ReconciliationRequestObject struct {
	Checksum        string `json:"checksum"`
	ResourceVersion int    `json:"resourceVersion"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
}

type ConnectedClientsMessage struct {
	ServerName string            `json:"serverName"`
	Clients    []ConnectedClient `json:"clients"`
	Timestamp  time.Time         `json:"keepalive"`
	Depth      int               `json:"depth"`
	MsgId      string            `json:"msgId"`
}

type ConnectedClient struct {
	Cluster             string    `json:"cluster"`
	Account             string    `json:"account"`
	SynchronizerVersion string    `json:"synchronizerVersion"`
	HelmVersion         string    `json:"helmVersion"`
	ConnectionId        string    `json:"connectionId"`
	ConnectionTime      time.Time `json:"connectionTime"`
	GitVersion          string    `json:"gitVersion"`
	CloudProvider       string    `json:"cloudProvider"`
}
