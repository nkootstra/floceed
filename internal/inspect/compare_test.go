package inspect

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCompareIgnoresVolatileManifestMetadata(t *testing.T) {
	baseline := testManifest(t, 3)
	current := testManifest(t, 3)
	baseline.Tool.Version = "v0.2.0"
	current.Tool.Version = "development"
	baseline.Capture.CapturedAt = time.Unix(1, 0).UTC()
	current.Capture.CapturedAt = time.Unix(999, 0).UTC()
	before, err := ProjectManifest(baseline)
	if err != nil {
		t.Fatal(err)
	}
	after, err := ProjectManifest(current)
	if err != nil {
		t.Fatal(err)
	}
	got := Compare(before, after)
	if got.Counts != (ReceiptCounts{Unchanged: 1}) || got.Resources[0].Outcome != OutcomeUnchanged {
		t.Fatalf("volatile metadata changed semantics: %#v", got)
	}
}

func TestCompareClassifiesAndSortsResourceOutcomes(t *testing.T) {
	baseline := Projection{Digest: "baseline", Resources: []ProjectedResource{
		{Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "removed"}, Selected: true},
		{Identity: ResourceIdentity{Service: "dynamodb", Type: "table", ID: "changed"}, Selected: true, StructureDigest: "old"},
		{Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "same"}, Selected: true},
	}}
	current := Projection{Digest: "current", Resources: []ProjectedResource{
		{Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "same"}, Selected: true},
		{Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "added"}, Selected: true},
		{Identity: ResourceIdentity{Service: "dynamodb", Type: "table", ID: "changed"}, Selected: true, StructureDigest: "new"},
	}}

	got := Compare(baseline, current)
	if got.Baseline != "baseline" || got.Current != "current" || got.Counts != (ReceiptCounts{Added: 1, Removed: 1, Changed: 1, Unchanged: 1}) {
		t.Fatalf("Compare() receipt = %#v", got)
	}
	want := []ResourceChange{
		{Resource: ResourceIdentity{Service: "dynamodb", Type: "table", ID: "changed"}, Outcome: OutcomeChanged, Categories: []ChangeCategory{CategoryStructure}},
		{Resource: ResourceIdentity{Service: "s3", Type: "bucket", ID: "added"}, Outcome: OutcomeAdded},
		{Resource: ResourceIdentity{Service: "s3", Type: "bucket", ID: "removed"}, Outcome: OutcomeRemoved},
		{Resource: ResourceIdentity{Service: "s3", Type: "bucket", ID: "same"}, Outcome: OutcomeUnchanged},
	}
	assertChanges(t, got.Resources, want)
}

func TestCompareReportsOnlyStableSemanticCategories(t *testing.T) {
	id := ResourceIdentity{Service: "dynamodb", Type: "table", ID: "customers"}
	baseline := Projection{Digest: "old", Source: SourceProjection{Region: "one"}, Target: TargetProjection{Image: "one"}, Resources: []ProjectedResource{{
		Identity: id, Selected: false, StructureDigest: "s1", DatasetDigest: "d1", GovernanceDigest: "g1", OperationsDigest: "o1", FindingsDigest: "f1",
	}}}
	current := Projection{Digest: "new", Source: SourceProjection{Region: "two"}, Target: TargetProjection{Image: "two"}, Resources: []ProjectedResource{{
		Identity: id, Selected: true, StructureDigest: "s2", DatasetDigest: "d2", GovernanceDigest: "g2", OperationsDigest: "o2", FindingsDigest: "f2",
	}}}
	wantBundle := []ChangeCategory{CategorySource, CategoryTarget}
	wantResource := []ChangeCategory{CategoryStructure, CategoryDataset, CategoryGovernance, CategoryOperations, CategoryFindings, CategorySelection, CategorySource, CategoryTarget}
	got := Compare(baseline, current)
	if len(got.Resources) != 1 || got.Resources[0].Outcome != OutcomeChanged {
		t.Fatalf("Compare() = %#v", got)
	}
	if strings.Join(categoryStrings(got.Categories), ",") != strings.Join(categoryStrings(wantBundle), ",") {
		t.Fatalf("receipt categories = %v, want %v", got.Categories, wantBundle)
	}
	if strings.Join(categories(got.Resources[0]), ",") != strings.Join(categoryStrings(wantResource), ",") {
		t.Fatalf("resource categories = %v, want %v", got.Resources[0].Categories, wantResource)
	}
	payload, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"reused", "invalidated", "RAW-STRUCTURE", "fixture-secret", "/private/path"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("receipt disclosed forbidden %q: %s", forbidden, payload)
		}
	}
}

func TestCompareReportsGovernanceOperationAndFindingChangesIndependently(t *testing.T) {
	id := ResourceIdentity{Service: "s3", Type: "bucket", ID: "assets"}
	tests := []struct {
		name   string
		mutate func(*ProjectedResource)
		want   ChangeCategory
	}{
		{"governance", func(r *ProjectedResource) { r.GovernanceDigest = "new" }, CategoryGovernance},
		{"operations", func(r *ProjectedResource) { r.OperationsDigest = "new" }, CategoryOperations},
		{"findings", func(r *ProjectedResource) { r.FindingsDigest = "new" }, CategoryFindings},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := ProjectedResource{Identity: id, GovernanceDigest: "old", OperationsDigest: "old", FindingsDigest: "old"}
			after := before
			test.mutate(&after)
			got := Compare(Projection{Digest: "old", Resources: []ProjectedResource{before}}, Projection{Digest: "new", Resources: []ProjectedResource{after}})
			if len(got.Resources) != 1 || len(got.Resources[0].Categories) != 1 || got.Resources[0].Categories[0] != test.want {
				t.Fatalf("Compare() = %#v, want resource category %q", got, test.want)
			}
		})
	}
}

func TestCompareExplainsBundleLevelSemanticChangesWithoutResources(t *testing.T) {
	baseline := Projection{
		Digest:     "old",
		Source:     SourceProjection{Region: "eu-west-1"},
		Target:     TargetProjection{Image: "floci:old"},
		Governance: &GovernanceSummary{Profile: "safe", PolicyIdentity: "policy-old"},
		Operations: []ProjectedOperation{{ID: "old-operation"}},
		Findings:   []Finding{{Code: "OLD_FINDING"}},
	}
	current := Projection{
		Digest:     "new",
		Source:     SourceProjection{Region: "eu-central-1"},
		Target:     TargetProjection{Image: "floci:new"},
		Governance: &GovernanceSummary{Profile: "safe", PolicyIdentity: "policy-new"},
		Operations: []ProjectedOperation{{ID: "new-operation"}},
		Findings:   []Finding{{Code: "NEW_FINDING"}},
	}

	got := Compare(baseline, current)
	want := []ChangeCategory{CategoryGovernance, CategoryOperations, CategoryFindings, CategorySource, CategoryTarget}
	if strings.Join(categoryStrings(got.Categories), ",") != strings.Join(categoryStrings(want), ",") {
		t.Fatalf("receipt categories = %v, want %v", got.Categories, want)
	}
	if len(got.Resources) != 0 || got.Counts != (ReceiptCounts{}) {
		t.Fatalf("unexpected resource changes: %#v", got)
	}
}

func TestCompareDeterministic(t *testing.T) {
	baseline := Projection{Digest: "same", Resources: []ProjectedResource{{Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "z"}}, {Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "a"}}}}
	current := Projection{Digest: "same", Resources: []ProjectedResource{{Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "a"}}, {Identity: ResourceIdentity{Service: "s3", Type: "bucket", ID: "z"}}}}
	want := `{"schema_version":1,"baseline":"same","current":"same","counts":{"added":0,"removed":0,"changed":0,"unchanged":2},"resources":[{"resource":{"service":"s3","type":"bucket","id":"a"},"outcome":"unchanged"},{"resource":{"service":"s3","type":"bucket","id":"z"},"outcome":"unchanged"}]}`
	got, err := json.Marshal(Compare(baseline, current))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("receipt = %s", got)
	}
}

func assertChanges(t *testing.T, got, want []ResourceChange) {
	t.Helper()
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(want)
	if string(a) != string(b) {
		t.Fatalf("changes = %s, want %s", a, b)
	}
}
func categories(change ResourceChange) []string { return categoryStrings(change.Categories) }
func categoryStrings(values []ChangeCategory) []string {
	out := make([]string, len(values))
	for i := range values {
		out[i] = string(values[i])
	}
	return out
}
