package domain

type BatchType string

//goland:noinspection GoUnusedConst
const (
	DefaultBatch        BatchType = ""
	ReconciliationBatch BatchType = "reconciliation"
)
