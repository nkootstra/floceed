package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

type Status string

const (
	StatusSuccess             Status = "success"
	StatusSuccessWithFindings Status = "success_with_findings"
	StatusPartial             Status = "partial"
	StatusError               Status = "error"
)

type Envelope struct {
	SchemaVersion int          `json:"schema_version"`
	Command       string       `json:"command"`
	Status        Status       `json:"status"`
	Data          any          `json:"data,omitempty"`
	Findings      any          `json:"findings,omitempty"`
	Error         *ErrorDetail `json:"error,omitempty"`
}
type ErrorDetail struct {
	Code        string `json:"code"`
	Message     string `json:"message"`
	Remediation string `json:"remediation,omitempty"`
}

func WriteJSON(w io.Writer, envelope Envelope) error {
	if envelope.SchemaVersion == 0 {
		envelope.SchemaVersion = 1
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(envelope)
}

type ErrorKind int

const (
	KindUnexpected ErrorKind = iota
	KindUsage
	KindSource
	KindPartial
	KindPlan
	KindFilesystem
	KindLocal
)

type CommandError struct {
	Kind        ErrorKind
	Code        string
	Message     string
	Remediation string
	Err         error
}

func (e *CommandError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Err.Error()
}
func (e *CommandError) Unwrap() error { return e.Err }
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return 130
	}
	var e *CommandError
	if !errors.As(err, &e) {
		return 1
	}
	switch e.Kind {
	case KindUsage:
		return 2
	case KindSource:
		return 3
	case KindPartial:
		return 4
	case KindPlan:
		return 5
	case KindFilesystem:
		return 6
	case KindLocal:
		return 7
	default:
		return 1
	}
}
func FormatError(err error) string {
	var e *CommandError
	if errors.As(err, &e) && e.Remediation != "" {
		return fmt.Sprintf("%s\nRemediation: %s", e.Error(), e.Remediation)
	}
	return err.Error()
}

// WriteInvocationError writes a machine-readable error when the invocation
// explicitly selected JSON output. The boolean reports whether JSON was
// requested, allowing callers to preserve their existing text error path.
func WriteInvocationError(cmd *cobra.Command, err error) (bool, error) {
	command, ok := jsonOutputCommand(cmd)
	if !ok {
		return false, nil
	}
	detail := ErrorDetail{Code: "UNEXPECTED", Message: err.Error()}
	var commandErr *CommandError
	if errors.As(err, &commandErr) {
		if commandErr.Code != "" {
			detail.Code = commandErr.Code
		}
		detail.Message = commandErr.Error()
		detail.Remediation = commandErr.Remediation
	} else if errors.Is(err, context.Canceled) {
		detail.Code = "CANCELED"
	}
	return true, WriteJSON(cmd.OutOrStdout(), Envelope{Command: command, Status: StatusError, Error: &detail})
}

func jsonOutputCommand(cmd *cobra.Command) (string, bool) {
	if flag := cmd.Flags().Lookup("output"); flag != nil && flag.Changed && flag.Value.String() == "json" {
		return cmd.Name(), true
	}
	for _, child := range cmd.Commands() {
		if name, ok := jsonOutputCommand(child); ok {
			return name, true
		}
	}
	return "", false
}
