package domain

type contextKey string

const (
	ContextKeyClientIdentifier contextKey = "clientIdentifier"
	ContextKeyDepth            contextKey = "depth"
	ContextKeyMsgId            contextKey = "msgId"
)
