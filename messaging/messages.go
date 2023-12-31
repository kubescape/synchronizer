package messaging

const (
	MsgPropTimestamp = "timestamp"
	// MsgPropCluster is the property name for the cluster name
	MsgPropCluster = "cluster"
	// MsgPropAccount is the property name for the account name
	MsgPropAccount = "account"
	// MsgPropEvent is the property name for the event type
	MsgPropEvent                            = "event"
	MsgPropEventValueGetObjectMessage       = "GetObject"
	MsgPropEventValuePatchObjectMessage     = "PatchObject"
	MsgPropEventValueVerifyObjectMessage    = "VerifyObject"
	MsgPropEventValueDeleteObjectMessage    = "DeleteObject"
	MsgPropEventValuePutObjectMessage       = "PutObject"
	MsgPropEventValueServerConnectedMessage = "ServerConnected"
)

type DeleteObjectMessage struct {
	Cluster   string `json:"cluster"`
	Account   string `json:"account"`
	Depth     int    `json:"depth"`
	Kind      string `json:"kind"`
	MsgId     string `json:"msgId"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type GetObjectMessage struct {
	BaseObject []byte `json:"baseObject"`
	Cluster    string `json:"cluster"`
	Account    string `json:"account"`
	Depth      int    `json:"depth"`
	Kind       string `json:"kind"`
	MsgId      string `json:"msgId"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
}

type NewChecksumMessage struct {
	Checksum  string `json:"checksum"`
	Cluster   string `json:"cluster"`
	Account   string `json:"account"`
	Depth     int    `json:"depth"`
	Kind      string `json:"kind"`
	MsgId     string `json:"msgId"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type NewObjectMessage struct {
	Cluster   string `json:"cluster"`
	Account   string `json:"account"`
	Depth     int    `json:"depth"`
	Kind      string `json:"kind"`
	MsgId     string `json:"msgId"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Object    []byte `json:"patch"`
}

type PatchObjectMessage struct {
	Checksum  string `json:"checksum"`
	Cluster   string `json:"cluster"`
	Account   string `json:"account"`
	Depth     int    `json:"depth"`
	Kind      string `json:"kind"`
	MsgId     string `json:"msgId"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Patch     []byte `json:"patch"`
}

type PutObjectMessage struct {
	Cluster   string `json:"cluster"`
	Account   string `json:"account"`
	Depth     int    `json:"depth"`
	Kind      string `json:"kind"`
	MsgId     string `json:"msgId"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Object    []byte `json:"patch"`
}

type VerifyObjectMessage struct {
	Checksum  string `json:"checksum"`
	Cluster   string `json:"cluster"`
	Account   string `json:"account"`
	Depth     int    `json:"depth"`
	Kind      string `json:"kind"`
	MsgId     string `json:"msgId"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type ServerConnectedMessage struct {
	Cluster string `json:"cluster"`
	Account string `json:"account"`
	Depth   int    `json:"depth"`
	MsgId   string `json:"msgId"`
}
