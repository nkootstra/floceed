package inspect

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nkootstra/floceed/internal/model"
)

func TestProjectManifestIgnoresVolatileMetadata(t *testing.T) {
	manifest := testManifest(t, 3)
	first, err := ProjectManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Tool.Version = "a-different-version"
	manifest.Capture.CapturedAt = manifest.Capture.CapturedAt.AddDate(1, 0, 0)
	second, err := ProjectManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("volatile metadata changed digest: %s != %s", first.Digest, second.Digest)
	}
}

func TestProjectManifestIsDeterministicAcrossUnorderedCollections(t *testing.T) {
	manifest := testManifest(t, 3)
	dynamo, err := model.NewSnapshot(model.ResourceRef{Service: "dynamodb", Type: "table", ID: "orders"}, "dynamodb", map[string]any{
		"name": "orders", "attribute_definitions": []any{}, "key_schema": []any{map[string]any{"attribute_name": "pk", "key_type": "HASH"}}, "billing_mode": "PAY_PER_REQUEST",
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest.Selected = append(manifest.Selected, dynamo.Resource)
	manifest.Snapshots = append(manifest.Snapshots, *dynamo)
	manifest.Snapshots[0].Dataset = &model.Dataset{Format: "s3-tar-gzip-v1", Records: 2, SourceBytes: 2, Chunks: []model.DataChunk{
		{Data: model.ArtifactRef{Path: "bundle/data/z", SHA256: "z", Size: 1}, Index: &model.ArtifactRef{Path: "bundle/data/z.index", SHA256: "zi", Size: 1}, Records: 1, SourceBytes: 1},
		{Data: model.ArtifactRef{Path: "bundle/data/a", SHA256: "a", Size: 1}, Index: &model.ArtifactRef{Path: "bundle/data/a.index", SHA256: "ai", Size: 1}, Records: 1, SourceBytes: 1},
	}}
	manifest.Operations = []model.Operation{
		{ID: "z", Stage: model.StageData, Service: "s3", ResourceID: "assets", Action: "load", DependsOn: []string{"b", "a"}},
		{ID: "a", Stage: model.StageBase, Service: "s3", ResourceID: "assets", Action: "create"},
	}
	manifest.Findings = []model.Finding{{Code: "Z", Severity: model.SeverityWarning, Support: model.SupportPartial}, {Code: "A", Severity: model.SeverityInfo, Support: model.SupportFull}}
	manifest.Governance = &model.GovernanceAudit{
		Profile: "safe", PolicyIdentity: "policy", KeyIDs: []string{"z", "a"}, Algorithms: []string{"pseudonym/v1", "hash/v1"},
		Rules:   []model.GovernanceRuleAudit{{RuleID: "z", Action: "omit", Count: model.CountBucketZero}, {RuleID: "a", Action: "hash", Count: model.CountBucket1To9}},
		Cohorts: []model.GovernanceCohortAudit{{ResourceIdentity: "z", Eligible: model.CountBucket1To9, Retained: model.CountBucket1To9}, {ResourceIdentity: "a", Eligible: model.CountBucketZero, Retained: model.CountBucketZero}},
	}
	first, err := ProjectManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Selected[0], manifest.Selected[1] = manifest.Selected[1], manifest.Selected[0]
	manifest.Snapshots[0].Dataset.Chunks[0], manifest.Snapshots[0].Dataset.Chunks[1] = manifest.Snapshots[0].Dataset.Chunks[1], manifest.Snapshots[0].Dataset.Chunks[0]
	manifest.Snapshots[0], manifest.Snapshots[1] = manifest.Snapshots[1], manifest.Snapshots[0]
	manifest.Operations[0], manifest.Operations[1] = manifest.Operations[1], manifest.Operations[0]
	manifest.Findings[0], manifest.Findings[1] = manifest.Findings[1], manifest.Findings[0]
	manifest.Governance.KeyIDs[0], manifest.Governance.KeyIDs[1] = manifest.Governance.KeyIDs[1], manifest.Governance.KeyIDs[0]
	manifest.Governance.Algorithms[0], manifest.Governance.Algorithms[1] = manifest.Governance.Algorithms[1], manifest.Governance.Algorithms[0]
	manifest.Governance.Rules[0], manifest.Governance.Rules[1] = manifest.Governance.Rules[1], manifest.Governance.Rules[0]
	manifest.Governance.Cohorts[0], manifest.Governance.Cohorts[1] = manifest.Governance.Cohorts[1], manifest.Governance.Cohorts[0]
	second, err := ProjectManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("shuffling changed digest: %s != %s", first.Digest, second.Digest)
	}
}

func TestProjectManifestSupportsLegacyChunkedAndGovernedSchemas(t *testing.T) {
	for _, schema := range []int{1, 2, 3} {
		manifest := testManifest(t, schema)
		if schema == 1 {
			manifest.Snapshots[0].Data = []model.ArtifactRef{{Path: "bundle/data/legacy", SHA256: "legacy-sha", Size: 12}}
		} else {
			manifest.Snapshots[0].Dataset = &model.Dataset{Format: "s3-tar-gzip-v1", Records: 1, SourceBytes: 12, Chunks: []model.DataChunk{{Data: model.ArtifactRef{Path: "bundle/data/chunk", SHA256: "chunk-sha", Size: 12}, Index: &model.ArtifactRef{Path: "bundle/data/index", SHA256: "index-sha", Size: 2}, Records: 1, SourceBytes: 12}}}
		}
		if schema == 3 {
			manifest.Governance = &model.GovernanceAudit{Profile: "safe", PolicyIdentity: "policy", Rules: []model.GovernanceRuleAudit{{RuleID: "opaque-rule", Action: "replace", Count: model.CountBucket1To9}}}
		}
		projection, err := ProjectManifest(manifest)
		if err != nil {
			t.Fatalf("schema %d: %v", schema, err)
		}
		if projection.Resources[0].DatasetDigest == "" {
			t.Fatalf("schema %d has no dataset digest", schema)
		}
	}
}

func TestProjectManifestComponentDigestsChangeIndependently(t *testing.T) {
	project := func(m model.Manifest) Projection {
		p, err := ProjectManifest(m)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	original := project(testManifest(t, 3)).Resources[0]
	structure := testManifest(t, 3)
	structure.Snapshots[0].Structure = json.RawMessage(`{"name":"assets","region":"eu-west-1","versioning":false}`)
	if got := project(structure).Resources[0]; got.StructureDigest == original.StructureDigest || got.DatasetDigest != original.DatasetDigest {
		t.Fatal("structure change escaped its component")
	}
	dataset := testManifest(t, 3)
	dataset.Snapshots[0].Dataset = testS3Dataset("bundle/data/private-value", "new-sha")
	if got := project(dataset).Resources[0]; got.DatasetDigest == original.DatasetDigest || got.StructureDigest != original.StructureDigest {
		t.Fatal("dataset change escaped its component")
	}
	governed := testManifest(t, 3)
	governed.Governance = &model.GovernanceAudit{Profile: "safe", PolicyIdentity: "policy-new"}
	if got := project(governed).Resources[0]; got.GovernanceDigest == original.GovernanceDigest {
		t.Fatal("governance digest did not change")
	}
}

func TestProjectManifestRejectsMalformedStructureAndAudit(t *testing.T) {
	manifest := testManifest(t, 3)
	manifest.Snapshots[0].Structure = json.RawMessage(`{"name":`)
	if _, err := ProjectManifest(manifest); err == nil {
		t.Fatal("expected malformed structure error")
	}
	manifest = testManifest(t, 3)
	manifest.Governance = &model.GovernanceAudit{Profile: "safe", PolicyIdentity: "policy", Rules: []model.GovernanceRuleAudit{{RuleID: "rule", Action: "replace", Count: "exact-secret-count"}}}
	if _, err := ProjectManifest(manifest); err == nil {
		t.Fatal("expected malformed audit error")
	}
}

func TestProjectManifestRejectsDuplicateSelectedResourceIdentitiesInEveryManifestSchema(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int
	}{{"schema-1", 1}, {"schema-2", 2}, {"schema-3", 3}} {
		t.Run(test.name, func(t *testing.T) {
			manifest := testManifest(t, test.version)
			manifest.Selected = append(manifest.Selected, manifest.Selected[0])
			if _, err := ProjectManifest(manifest); err == nil {
				t.Fatal("expected duplicate selected identity to prevent inspection")
			}
		})
	}
}

func TestPublicContractsDoNotDiscloseFixtureOrGovernanceSecrets(t *testing.T) {
	manifest := testManifest(t, 3)
	manifest.Snapshots[0].Structure = json.RawMessage(`{"name":"assets","region":"eu-west-1","fixture":"FIXTURE-CANARY"}`)
	manifest.Snapshots[0].Dataset = testS3Dataset("bundle/data/REPLACEMENT-CANARY", "safe-digest")
	manifest.Governance = &model.GovernanceAudit{Profile: "safe", PolicyIdentity: "policy", KeyIDs: []string{"non-secret-key-id"}}
	projection, err := ProjectManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	receipt := Receipt{SchemaVersion: ReceiptSchemaVersion, Current: projection.Digest, Resources: []ResourceChange{{Resource: projection.Resources[0].Identity, Outcome: OutcomeChanged, Categories: []ChangeCategory{CategoryStructure, CategoryDataset, CategoryGovernance}}}}
	inspection := Inspection{SchemaVersion: InspectionSchemaVersion, Valid: true, BundleIdentity: projection.Digest, Receipt: &receipt}
	for name, value := range map[string]any{"projection": projection, "receipt": receipt, "inspection": inspection} {
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		for _, canary := range []string{"FIXTURE-CANARY", "REPLACEMENT-CANARY", "salt-canary", "audit-sample-canary"} {
			if strings.Contains(string(payload), canary) {
				t.Fatalf("%s disclosed %q: %s", name, canary, payload)
			}
		}
	}
}

func testS3Dataset(path, digest string) *model.Dataset {
	return &model.Dataset{Format: "s3-tar-gzip-v1", Records: 1, SourceBytes: 1, Chunks: []model.DataChunk{{
		Data:    model.ArtifactRef{Path: path, SHA256: digest, Size: 1},
		Index:   &model.ArtifactRef{Path: path + ".index", SHA256: digest + "-index", Size: 1},
		Records: 1, SourceBytes: 1,
	}}}
}

func TestReceiptVocabularyIsStableAndClosed(t *testing.T) {
	outcomes := []Outcome{OutcomeAdded, OutcomeRemoved, OutcomeChanged, OutcomeUnchanged}
	if got := strings.Join([]string{string(outcomes[0]), string(outcomes[1]), string(outcomes[2]), string(outcomes[3])}, ","); got != "added,removed,changed,unchanged" {
		t.Fatalf("outcomes = %s", got)
	}
	categories := []ChangeCategory{CategoryStructure, CategoryDataset, CategoryGovernance, CategoryOperations, CategoryFindings, CategorySelection, CategorySource, CategoryTarget}
	values := make([]string, len(categories))
	for i, value := range categories {
		values[i] = string(value)
	}
	if got := strings.Join(values, ","); got != "structure,dataset,governance,operations,findings,selection,source,target" {
		t.Fatalf("categories = %s", got)
	}
}

func testManifest(t *testing.T, schema int) model.Manifest {
	t.Helper()
	snapshot, err := model.NewSnapshot(model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}, "s3", map[string]any{
		"name": "assets", "region": "eu-west-1", "versioning": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return model.Manifest{
		SchemaVersion: schema,
		Tool:          model.ToolMetadata{Version: "v0.2.0"},
		Target:        model.TargetMetadata{FlociVersion: "1.6.0", Image: "floci:1.6.0"},
		Source:        model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"},
		Selected:      []model.ResourceRef{snapshot.Resource},
		Snapshots:     []model.Snapshot{*snapshot},
	}
}
