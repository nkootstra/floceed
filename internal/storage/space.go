package storage

import (
	"fmt"
	"golang.org/x/sys/unix"
	"math"
)

const minimumHeadroom int64 = 1 << 30

type InsufficientSpaceError struct {
	Path                string
	Required, Available int64
}

func (e *InsufficientSpaceError) Error() string {
	return fmt.Sprintf("insufficient disk space at %s: need %d bytes, %d bytes available", e.Path, e.Required, e.Available)
}

// RequireAvailable checks conservative working space before a full-data transfer.
func RequireAvailable(path string, payload, workingCopies int64) error {
	if payload < 0 {
		payload = 0
	}
	if payload == 0 {
		return nil
	}
	if workingCopies < 1 {
		workingCopies = 1
	}
	required := payload
	if payload > math.MaxInt64/workingCopies {
		required = math.MaxInt64
	} else {
		required = payload * workingCopies
	}
	headroom := required / 100 * 15
	if headroom < minimumHeadroom {
		headroom = minimumHeadroom
	}
	if required > math.MaxInt64-headroom {
		required = math.MaxInt64
	} else {
		required += headroom
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return err
	}
	available := int64(stat.Bavail) * int64(stat.Bsize)
	if available < required {
		return &InsufficientSpaceError{Path: path, Required: required, Available: available}
	}
	return nil
}
