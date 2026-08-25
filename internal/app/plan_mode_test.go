package app

import "testing"

func TestCaptureModePredicates(t *testing.T) {
	tests := []struct {
		mode          captureMode
		plans, copies bool
	}{
		{structureOnly, false, false},
		{planData, true, false},
		{captureData, true, true},
	}
	for _, tt := range tests {
		if got := tt.mode.plansData(); got != tt.plans {
			t.Errorf("mode %d plansData() = %v, want %v", tt.mode, got, tt.plans)
		}
		if got := tt.mode.capturesData(); got != tt.copies {
			t.Errorf("mode %d capturesData() = %v, want %v", tt.mode, got, tt.copies)
		}
	}
}
