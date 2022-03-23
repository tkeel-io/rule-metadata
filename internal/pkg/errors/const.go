package errors

const (
	Root   = "MDMP."
	system = Root + "System."
	common = Root + "Common."
	entity = Root + "Entity."
	rule   = Root + "Rule."
	pubsub = Root + "Subscription."
)

const (
	NotExisted = "NotExisted"
	Invalid    = "Invalid"
	Forbidden  = "Forbidden"
)

const (
	SystemError               = system + "SystemError"
	SystemInternalError       = system + "InternalError"
	SystemInternalParamsError = system + "InternalParamsError"
)

const (
	EntityNotExisted         = entity + NotExisted
	EntityInvalid            = entity + Invalid
	EntityPropertyNotExisted = entity + "Property" + NotExisted
	EntityParamsInvalid      = entity + "Params" + Invalid
	RuleInvalid              = rule + Invalid
	RulePropertyNotExisted   = rule + "Property" + NotExisted
	RuleParamsInvalid        = rule + "Params" + Invalid
)
