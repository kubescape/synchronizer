package domain

type Strategy string

//goland:noinspection GoUnusedConst
const (
	CopyStrategy  Strategy = "copy"
	PatchStrategy Strategy = "patch"
)
