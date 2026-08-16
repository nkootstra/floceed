package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nkootstra/floceed/internal/testfixture"
)

func TestFixtureCommandsVerifyAndAdmitOffline(t *testing.T) {
	root := t.TempDir()
	if err := testfixture.GenerateInspectFixtures(root); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(root, "current", ".floceed")
	policyPath := filepath.Join(root, "policy.yaml")
	if err := os.WriteFile(policyPath, []byte("schema_version: 1\nallowed_accounts: [\"123456789012\"]\nallowed_finding_codes: [FIXTURE_METADATA_ONLY]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"verify", "--input", input, "--output", "json"}, {"admit", "--input", input, "--policy", policyPath, "--output", "json"}} {
		cmd := fixtureCommand()
		var output bytes.Buffer
		cmd.SetOut(&output)
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		var envelope Envelope
		if err := json.Unmarshal(output.Bytes(), &envelope); err != nil {
			t.Fatalf("%v output: %v", args, err)
		}
		if envelope.Status != StatusSuccess {
			t.Fatalf("%v status = %s", args, envelope.Status)
		}
	}
}
