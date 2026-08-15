package app

import (
	"errors"
	"os"
)

type ErrorKind string

const (
	ErrorUsage      ErrorKind = "usage"
	ErrorSource     ErrorKind = "source"
	ErrorPartial    ErrorKind = "partial"
	ErrorPlan       ErrorKind = "plan"
	ErrorFilesystem ErrorKind = "filesystem"
	ErrorLocal      ErrorKind = "local"
)

type Error struct {
	Kind        ErrorKind
	Code        string
	Message     string
	Remediation string
	Err         error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "floceed operation failed"
}

func (e *Error) Unwrap() error { return e.Err }

func sourceError(err error) error {
	var appError *Error
	if errors.As(err, &appError) {
		return err
	}
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return filesystemError(err)
	}
	return &Error{Kind: ErrorSource, Code: "AWS_SOURCE_FAILED", Message: err.Error(), Err: err}
}

func filesystemError(err error) error {
	return &Error{Kind: ErrorFilesystem, Code: "BUNDLE_FAILED", Message: err.Error(), Err: err}
}
