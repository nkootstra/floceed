package model

import (
	"encoding/json"
	"os"
	"testing"
)

func TestManifestValidateRejectsNewerSchema(t *testing.T) {
	m := Manifest{SchemaVersion: CurrentManifestSchemaVersion + 1}
	if err := m.Validate(); err == nil {
		t.Fatal("expected newer schema to be rejected")
	}
}

func TestManifestContractV1GoldenFixture(t *testing.T) {
	payload, err := os.ReadFile("../../runtime/testdata/manifest-contract-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		t.Fatal(err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("golden contract should validate: %v", err)
	}
	if len(manifest.Snapshots) != 2 {
		t.Fatalf("snapshots = %d, want 2", len(manifest.Snapshots))
	}
}

func TestManifestValidateRejectsUnsupportedSnapshotContracts(t *testing.T) {
	valid, err := NewSnapshot(ResourceRef{Service: "s3", ID: "assets"}, "s3", map[string]any{"name": "assets", "region": "eu-west-1"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []Snapshot{
		{Resource: ResourceRef{Service: "s3"}, Service: "s3", StructureVersion: CurrentSnapshotStructureVersion + 1, Structure: valid.Structure},
		{Resource: ResourceRef{Service: "s3"}, Service: "dynamodb", StructureVersion: CurrentSnapshotStructureVersion, Structure: valid.Structure},
		{Resource: ResourceRef{Service: "s3"}, Service: "s3", StructureVersion: CurrentSnapshotStructureVersion, Structure: json.RawMessage(`{"name":"assets"}`)},
	}
	for _, snapshot := range cases {
		manifest := Manifest{SchemaVersion: CurrentManifestSchemaVersion, Snapshots: []Snapshot{snapshot}}
		if err := manifest.Validate(); err == nil {
			t.Fatalf("expected snapshot to be rejected: %#v", snapshot)
		}
	}
}

func TestFindingJSONUsesStableFields(t *testing.T) {
	b, err := json.Marshal(Finding{Code: "S3_REPLICATION_UNSUPPORTED", Severity: SeverityWarning, Support: SupportTargetUnsupported})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"code":"S3_REPLICATION_UNSUPPORTED","severity":"warning","support":"target_unsupported"}`
	if string(b) != want {
		t.Fatalf("got %s, want %s", b, want)
	}
}

func TestManifestSchemaV3GovernanceAuditUsesDisclosureBuckets(t *testing.T) {
	manifest := Manifest{
		SchemaVersion: 3,
		Governance: &GovernanceAudit{
			Profile: "safe", PolicyIdentity: "policy-identity",
			KeyIDs: []string{"fixtures-2026-08"}, Algorithms: []string{"pseudonym/v1"},
			Rules: []GovernanceRuleAudit{{RuleID: "rule-001", Action: "pseudonymize", Count: CountBucket1To9}},
		},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("schema-3 manifest should validate: %v", err)
	}
	payload, err := json.Marshal(manifest.Governance)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(payload), `{"profile":"safe","policy_identity":"policy-identity","key_ids":["fixtures-2026-08"],"algorithms":["pseudonym/v1"],"rules":[{"rule_id":"rule-001","action":"pseudonymize","count":"1-9"}]}`; got != want {
		t.Fatalf("governance JSON = %s, want %s", got, want)
	}

	manifest.Governance.Rules[0].Count = "7"
	if err := manifest.Validate(); err == nil {
		t.Fatal("expected exact governance count to be rejected")
	}
}

func TestManifestOlderSchemasRemainValidWithoutGovernance(t *testing.T) {
	for _, version := range []int{1, 2} {
		manifest := Manifest{SchemaVersion: version}
		if err := manifest.Validate(); err != nil {
			t.Fatalf("schema %d should remain valid: %v", version, err)
		}
	}
}

func TestManifestSchemaV3RejectsInvalidGovernanceAuditShape(t *testing.T) {
	valid := func() *GovernanceAudit {
		return &GovernanceAudit{
			Profile: "safe", PolicyIdentity: "policy-identity",
			KeyIDs: []string{"key-1"}, Algorithms: []string{"pseudonym/v1"},
			Rules:   []GovernanceRuleAudit{{RuleID: "rule-1", Action: "pseudonymize", Count: CountBucket1To9}},
			Cohorts: []GovernanceCohortAudit{{ResourceIdentity: "resource-1", Eligible: CountBucket1To9, Retained: CountBucket1To9}},
		}
	}
	tests := map[string]func(*GovernanceAudit){
		"duplicate key ID":          func(a *GovernanceAudit) { a.KeyIDs = []string{"key-1", "key-1"} },
		"empty algorithm":           func(a *GovernanceAudit) { a.Algorithms = []string{""} },
		"unsupported action":        func(a *GovernanceAudit) { a.Rules[0].Action = "custom" },
		"duplicate rule ID":         func(a *GovernanceAudit) { a.Rules = append(a.Rules, a.Rules[0]) },
		"duplicate cohort identity": func(a *GovernanceAudit) { a.Cohorts = append(a.Cohorts, a.Cohorts[0]) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			audit := valid()
			mutate(audit)
			if err := (Manifest{SchemaVersion: 3, Governance: audit}).Validate(); err == nil {
				t.Fatal("expected invalid governance audit to be rejected")
			}
		})
	}
}
