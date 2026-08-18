package storage

import (
	"errors"
	"math"
	"testing"
)

func TestRequireAvailableAcceptsZeroAndModeratePayloads(t *testing.T) {
	dir := t.TempDir()
	if err := RequireAvailable(dir, 0, 1); err != nil {
		t.Fatalf("zero payload must be accepted: %v", err)
	}
	// 1 MiB is far below the 1 GiB headroom floor on any real filesystem.
	if err := RequireAvailable(dir, 1<<20, 1); err != nil {
		t.Fatalf("moderate payload rejected: %v", err)
	}
}

func TestRequireAvailableOverflowBecomesInsufficientSpace(t *testing.T) {
	var err *InsufficientSpaceError
	// A payload large enough to saturate the required computation must report
	// insufficient space against any real filesystem.
	got := RequireAvailable(t.TempDir(), math.MaxInt64/2, 4)
	if !errors.As(got, &err) {
		t.Fatalf("overflow payload error = %v, want *InsufficientSpaceError", got)
	}
	if err.Required != math.MaxInt64 {
		t.Fatalf("required = %d, want math.MaxInt64", err.Required)
	}
}
