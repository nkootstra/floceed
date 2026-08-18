package testfixture

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nkootstra/floceed/internal/model"
)

func TestGenerateRepresentativeBundleIncludesDataAndStructureOnlyServices(t *testing.T) {
	root := t.TempDir()
	if err := GenerateRepresentativeBundle(root); err != nil {
		t.Fatalf("generate representative bundle: %v", err)
	}
	for _, path := range []string{
		".floceed/bundle/data/dynamodb/items-000001.ndjson",
		".floceed/bundle/data/kinesis/floceed-example-stream.ndjson",
		".floceed/bundle/data/sqs/floceed-example-events.ndjson",
		".floceed/bundle/data/s3/pack-000001.tar.gz",
		".floceed/bundle/data/s3/pack-000001.index.gz",
		".floceed/bundle/manifest.json",
		".floceed/checksums.json",
	} {
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatalf("expected generated file %s: %v", path, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, ".floceed/bundle/manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("generated manifest is invalid: %v", err)
	}
	if len(manifest.Selected) != 5 || len(manifest.Snapshots) != 5 {
		t.Fatalf("generated manifest selected=%d snapshots=%d, want five each", len(manifest.Selected), len(manifest.Snapshots))
	}
	for _, snapshot := range manifest.Snapshots {
		if snapshot.Service == "sns" && snapshot.Dataset != nil {
			t.Fatalf("structure-only %s snapshot unexpectedly contains dataset", snapshot.Service)
		}
	}
}
