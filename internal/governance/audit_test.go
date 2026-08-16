package governance

import "testing"

func TestAuditBucketsExactCounts(t *testing.T) {
	tests := []struct {
		count int
		want  CountBucket
	}{{0, BucketZero}, {1, BucketOneToNine}, {9, BucketOneToNine}, {10, BucketTenToNinetyNine}, {99, BucketTenToNinetyNine}, {100, BucketHundredToNineHundredNinetyNine}, {999, BucketHundredToNineHundredNinetyNine}, {1000, BucketThousandPlus}}
	for _, test := range tests {
		if got := BucketCount(test.count); got != test.want {
			t.Errorf("BucketCount(%d) = %q, want %q", test.count, got, test.want)
		}
	}
}

func TestAuditSnapshotExposesOnlyOpaqueRuleIDsAndBuckets(t *testing.T) {
	audit := NewAudit()
	audit.Record("rule-002")
	for range 10 {
		audit.Record("rule-001")
	}

	want := []RuleAudit{{RuleID: "rule-001", Count: BucketTenToNinetyNine}, {RuleID: "rule-002", Count: BucketOneToNine}}
	got := audit.Snapshot()
	if len(got) != len(want) {
		t.Fatalf("snapshot = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("snapshot = %#v, want %#v", got, want)
		}
	}
}
