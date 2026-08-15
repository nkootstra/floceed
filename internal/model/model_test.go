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
