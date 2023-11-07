package core

import (
	beServerV1 "github.com/kubescape/backend/pkg/server/v1"
)

// These headers are required for client-server authentication
const (
	AccessKeyHeader   = beServerV1.AccessKeyHeader
	AccountHeader     = "Account"
	ClusterNameHeader = "ClusterName"
)
