// Package testfixture creates deterministic, metadata-only generated bundles
// for offline tests and examples. It never contacts AWS, Docker, or Floci.
package testfixture

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/model"
)

const projectYAML = `schema_version: 1
source:
  region: eu-west-1
  expected_account_id: "123456789012"
target:
  floci_version: 1.6.0
output:
  directory: .floceed
`

var capturedAt = time.Date(2026, 8, 16, 10, 21, 0, 0, time.UTC)

// GenerateInspectFixtures writes the committed offline comparison fixtures.
func GenerateInspectFixtures(root string) error {
	baseline := manifest("baseline")
	current := manifest("current")
	governanceCurrent := manifest("current")
	governanceCurrent.Governance.PolicyIdentity = "policy-fixture-v2"
	governanceCurrent.Governance.Rules[0].Count = model.CountBucket100To999
	for name, value := range map[string]model.Manifest{
		"baseline": baseline, "current": current, "governance-current": governanceCurrent,
	} {
		if err := writeProjectBundle(filepath.Join(root, name), value); err != nil {
			return fmt.Errorf("write %s fixture: %w", name, err)
		}
	}
	return nil
}

// GenerateSchemaFixture writes a small valid bundle for a supported manifest schema.
func GenerateSchemaFixture(root string, schema int) error {
	value := manifest("current")
	value.SchemaVersion = schema
	if schema < 3 {
		value.Governance = nil
	}
	if schema == 1 {
		for index := range value.Snapshots {
			value.Snapshots[index].Dataset = nil
		}
	}
	return writeGenerated(root, value)
}

func writeProjectBundle(root string, value model.Manifest) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "floceed.yaml"), []byte(projectYAML), 0o600); err != nil {
		return err
	}
	return writeGenerated(filepath.Join(root, ".floceed"), value)
}

func writeGenerated(root string, value model.Manifest) error {
	if err := value.Validate(); err != nil {
		return err
	}
	files := map[string][]byte{
		".gitignore":                []byte("bundle/data/\n"),
		"compose.generated.yaml":    []byte("services: {}\n"),
		"runtime/replay.py":         []byte("# deterministic metadata-only fixture\n"),
		"init/ready.d/10-replay.py": []byte("# deterministic metadata-only fixture\n"),
	}
	encoded, err := bundle.CanonicalJSON(value)
	if err != nil {
		return err
	}
	files["bundle/manifest.json"] = encoded
	for name, contents := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			return err
		}
	}
	sums, err := bundle.BuildChecksums(root, "checksums.json")
	if err != nil {
		return err
	}
	encoded, err = bundle.CanonicalJSON(sums)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "checksums.json"), encoded, 0o600)
}

func manifest(version string) model.Manifest {
	refs := map[string]model.ResourceRef{
		"changed":   {Service: "dynamodb", Type: "table", ID: "customers"},
		"removed":   {Service: "dynamodb", Type: "table", ID: "legacy-orders"},
		"added":     {Service: "s3", Type: "bucket", ID: "reports"},
		"unchanged": {Service: "s3", Type: "bucket", ID: "assets"},
	}
	selected := []model.ResourceRef{refs["changed"], refs["unchanged"]}
	snapshots := []model.Snapshot{
		dynamoSnapshot(refs["changed"], "PAY_PER_REQUEST"),
		s3Snapshot(refs["unchanged"]),
	}
	if version == "baseline" {
		selected = append(selected, refs["removed"])
		snapshots = append(snapshots, dynamoSnapshot(refs["removed"], "PAY_PER_REQUEST"))
	} else {
		selected = append(selected, refs["added"])
		snapshots[0] = dynamoSnapshot(refs["changed"], "PROVISIONED")
		snapshots = append(snapshots, s3Snapshot(refs["added"]))
	}
	return model.Manifest{
		SchemaVersion: model.CurrentManifestSchemaVersion,
		Tool:          model.ToolMetadata{Version: "v0.3.0-fixture"},
		Target:        model.TargetMetadata{FlociVersion: "1.6.0", Image: "ghcr.io/example/floci:1.6.0"},
		Source:        model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"},
		Capture:       model.CaptureMetadata{CapturedAt: capturedAt},
		Selected:      selected, Snapshots: snapshots,
		Operations: []model.Operation{{ID: "create-customers", Stage: model.StageBase, Service: "dynamodb", ResourceID: "customers", Action: "create_table"}},
		Findings: []model.Finding{{Code: "FIXTURE_METADATA_ONLY", Severity: model.SeverityInfo, Support: model.SupportFull,
			Message: "GOVERNANCE_REPLACEMENT_SECRET_CANARY", Remediation: "FIXTURE_RECORD_SECRET_CANARY"}},
		Governance: governanceAudit(),
	}
}

func governanceAudit() *model.GovernanceAudit {
	return &model.GovernanceAudit{
		Profile: "share-safe", PolicyIdentity: "policy-fixture-v1", CohortIdentity: "cohort-fixture-v1",
		KeyIDs: []string{"fixture-key-id"}, Algorithms: []string{"pseudonym/v1"},
		Rules:   []model.GovernanceRuleAudit{{RuleID: "customer-email", Action: "pseudonymize", Count: model.CountBucket10To99}},
		Cohorts: []model.GovernanceCohortAudit{{ResourceIdentity: "dynamodb/table/customers", Eligible: model.CountBucket100To999, Retained: model.CountBucket10To99, Truncated: true}},
	}
}

func dynamoSnapshot(ref model.ResourceRef, billing string) model.Snapshot {
	format := "dynamodb-ndjson-v1"
	if billing == "PROVISIONED" {
		format = "dynamodb-ndjson-gzip-v1"
	}
	return model.Snapshot{
		Resource: ref, Service: "dynamodb", StructureVersion: 1,
		Structure: []byte(fmt.Sprintf(`{"name":%q,"attribute_definitions":[],"key_schema":[{"attribute_name":"id","key_type":"HASH"}],"billing_mode":%q,"private_fixture_canary":"FIXTURE_RECORD_SECRET_CANARY"}`, ref.ID, billing)),
		Dataset:   &model.Dataset{Format: format, Consistency: "eventual"},
	}
}

func s3Snapshot(ref model.ResourceRef) model.Snapshot {
	return model.Snapshot{
		Resource: ref, Service: "s3", StructureVersion: 1,
		Structure: []byte(fmt.Sprintf(`{"name":%q,"region":"eu-west-1"}`, ref.ID)),
		Dataset:   &model.Dataset{Format: "s3-tar-gzip-v1", Consistency: "point-in-time"},
	}
}
