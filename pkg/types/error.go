package types

import (
	"errors"
	"fmt"
)

//
//type Error struct {
//	Status  int    `json:"status"`
//	Message string `json:"message"`
//}
//
//func (e *Error) Error() string { return e.Message }

var (
	ErrRuleNil              = New("rule nil")
	ErrMarshalError         = New("marshal error")
	ErrUnmarshalError       = New("unmarshal error")
	ErrEntityNilError       = New("entity nil")
	ErrSubscriptionNilError = New("subscription nil")
	ErrRuleParseError       = New("rule parse error")
	ErrRuleUserIdEmpty      = New("userid empty")
	ErrRuleUserIdForbidden  = New("userid forbidden")
	ErrRuleTopicNilError    = New("rule get topic fail")
)

// New returns an error that formats as the given text.
// Each call to New returns a distinct error value even if the text is identical.
func New(text string) error {
	return errors.New(text)
}

// Errorf formats according to a format specifier and returns the string as a
// value that satisfies error.
//
// If the format specifier includes a %w verb with an error operand,
// the returned error will implement an Unwrap method returning the operand. It is
// invalid to include more than one %w verb or to supply it with an operand
// that does not implement the error interface. The %w verb is otherwise
// a synonym for %v.
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}
