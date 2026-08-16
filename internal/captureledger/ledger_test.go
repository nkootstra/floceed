package captureledger

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCaptureDefinitionDigestNormalizesDefaultsAndOrdering(t *testing.T) {
	left := CaptureDefinition{Source: SourceIdentity{AccountID: "123456789012", Region: "eu-west-1"}, Resource: ResourceDescriptor{Service: "s3", Type: "bucket", ID: "assets"}, Prefixes: []string{"logs/", ""}, DatasetFormat: "s3-tar-gzip-v1", StructureVersion: 1}
	right := left
	right.Mode = "bounded"
	right.Overwrite = "if-different"
	right.Prefixes = []string{"", "logs/", "logs/"}

	want, err := DigestCaptureDefinition(left)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DigestCaptureDefinition(right)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("normalized digests differ: %s != %s", got, want)
	}
}

func TestCaptureDefinitionDigestOwnsEveryCaptureField(t *testing.T) {
	base := CaptureDefinition{Source: SourceIdentity{AccountID: "123456789012", Region: "eu-west-1"}, Resource: ResourceDescriptor{Service: "s3", Type: "bucket", ID: "assets"}, Mode: "bounded", Prefixes: []string{"logs/"}, Limits: Limits{MaxObjects: 1, MaxItems: 2, MaxPages: 3, MaxObjectBytes: 4, MaxTotalBytes: 5}, Overwrite: "if-different", Gzip: true, PreserveProvisioned: true, AllowPartialData: true, PolicyIdentity: strings.Repeat("a", 64), DatasetFormat: "s3-tar-gzip-v1", DatasetVersion: 2, StructureVersion: 1}
	want, err := DigestCaptureDefinition(base)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*CaptureDefinition){
		"account": func(d *CaptureDefinition) { d.Source.AccountID = "210987654321" }, "region": func(d *CaptureDefinition) { d.Source.Region = "us-east-1" },
		"service": func(d *CaptureDefinition) { d.Resource.Service = "dynamodb" }, "type": func(d *CaptureDefinition) { d.Resource.Type = "table" }, "resource": func(d *CaptureDefinition) { d.Resource.ID = "other" },
		"mode": func(d *CaptureDefinition) { d.Mode = "full" }, "prefixes": func(d *CaptureDefinition) { d.Prefixes = []string{"other/"} }, "max objects": func(d *CaptureDefinition) { d.Limits.MaxObjects++ },
		"max items": func(d *CaptureDefinition) { d.Limits.MaxItems++ }, "max pages": func(d *CaptureDefinition) { d.Limits.MaxPages++ }, "max object bytes": func(d *CaptureDefinition) { d.Limits.MaxObjectBytes++ }, "max total bytes": func(d *CaptureDefinition) { d.Limits.MaxTotalBytes++ },
		"overwrite": func(d *CaptureDefinition) { d.Overwrite = "always" }, "gzip": func(d *CaptureDefinition) { d.Gzip = false }, "preserve provisioned": func(d *CaptureDefinition) { d.PreserveProvisioned = false }, "allow partial": func(d *CaptureDefinition) { d.AllowPartialData = false },
		"policy": func(d *CaptureDefinition) { d.PolicyIdentity = strings.Repeat("b", 64) }, "format": func(d *CaptureDefinition) { d.DatasetFormat = "dynamodb-jsonl-v2" }, "dataset version": func(d *CaptureDefinition) { d.DatasetVersion++ }, "structure version": func(d *CaptureDefinition) { d.StructureVersion++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			changed := base
			changed.Prefixes = append([]string(nil), base.Prefixes...)
			mutate(&changed)
			got, err := DigestCaptureDefinition(changed)
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				t.Fatalf("field change did not change digest %s", got)
			}
		})
	}
}

func validGeneration() Generation {
	return Generation{SchemaVersion: CurrentSchemaVersion, ID: strings.Repeat("1", 64), Source: SourceIdentity{AccountID: "123456789012", Region: "eu-west-1"}, CreatedAt: time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC), CompletedAt: time.Date(2026, 8, 16, 10, 1, 0, 0, time.UTC), Resources: []Resource{{Descriptor: ResourceDescriptor{Service: "s3", Type: "bucket", ID: "assets"}, CaptureDefinition: strings.Repeat("2", 64), Units: []Unit{{ID: "pack-000001", Freshness: FreshnessEvidence{Kind: "s3_inventory", Digest: strings.Repeat("3", 64)}, Artifacts: []Artifact{{Path: "data/s3/assets/pack-000001.tar.gz", SHA256: strings.Repeat("4", 64), Size: 12, MediaType: "application/gzip"}}, Outcome: UnitOutcomeReused, Reason: ReasonReused, CapturedAt: time.Date(2026, 8, 16, 9, 0, 0, 0, time.UTC)}}}}}
}

func TestGenerationValidateRejectsUnsafeOrAmbiguousMetadata(t *testing.T) {
	tests := map[string]func(*Generation){
		"unknown schema": func(g *Generation) { g.SchemaVersion++ }, "unsafe path": func(g *Generation) { g.Resources[0].Units[0].Artifacts[0].Path = "../payload" },
		"duplicate unit": func(g *Generation) { g.Resources[0].Units = append(g.Resources[0].Units, g.Resources[0].Units[0]) }, "duplicate artifact": func(g *Generation) {
			g.Resources[0].Units[0].Artifacts = append(g.Resources[0].Units[0].Artifacts, g.Resources[0].Units[0].Artifacts[0])
		},
		"negative size": func(g *Generation) { g.Resources[0].Units[0].Artifacts[0].Size = -1 }, "bad digest": func(g *Generation) { g.Resources[0].Units[0].Artifacts[0].SHA256 = "abcd" },
		"unsupported reason": func(g *Generation) { g.Resources[0].Units[0].Reason = Reason("mystery") }, "unsupported outcome": func(g *Generation) { g.Resources[0].Units[0].Outcome = UnitOutcome("cached") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			g := validGeneration()
			mutate(&g)
			if err := g.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestGenerationCanonicalJSONIsDeterministicAndReceiptSafe(t *testing.T) {
	a := validGeneration()
	b := validGeneration()
	a.Resources[0].Units[0].Freshness.Records, a.Resources[0].Units[0].Freshness.Bytes = 2, 7
	b.Resources[0].Units[0].Freshness.Records, b.Resources[0].Units[0].Freshness.Bytes = 2, 7
	b.Resources = append([]Resource{{Descriptor: ResourceDescriptor{Service: "dynamodb", Type: "table", ID: "orders"}, CaptureDefinition: strings.Repeat("5", 64), Units: []Unit{}}}, b.Resources...)
	a.Resources = append(a.Resources, b.Resources[0])
	a.Resources[0], a.Resources[1] = a.Resources[1], a.Resources[0]

	one, err := a.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	two, err := b.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(one) != string(two) {
		t.Fatalf("canonical JSON differs:\n%s\n%s", one, two)
	}
	for _, forbidden := range []string{"profile", "project", "payload", "body", "items", "contents"} {
		if strings.Contains(strings.ToLower(string(one)), forbidden) {
			t.Fatalf("serialized metadata contains payload-bearing field %q: %s", forbidden, one)
		}
	}
	var roundTrip Generation
	if err := json.Unmarshal(one, &roundTrip); err != nil {
		t.Fatal(err)
	}
	again, err := roundTrip.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(one) {
		t.Fatalf("round trip changed canonical JSON")
	}
}

func TestStableReasonsAreExhaustiveAndValid(t *testing.T) {
	for _, reason := range []Reason{ReasonNoCandidate, ReasonCaptureDefinitionChanged, ReasonFormatChanged, ReasonSourceContentChanged, ReasonSourceUnitMissing, ReasonFreshnessUnproven, ReasonArtifactMissing, ReasonArtifactCorrupt, ReasonReused} {
		if !reason.Valid() {
			t.Fatalf("reason %q should be valid", reason)
		}
	}
}
