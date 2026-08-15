package awsconfig

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aws/smithy-go"
)

func TestProfilesReadsOnlyProfileSections(t *testing.T) {
	d := t.TempDir()
	config := filepath.Join(d, "config")
	credentials := filepath.Join(d, "credentials")
	mustWrite(t, config, "[default]\nregion=x\n[profile work]\nsso_session=corp\n[sso-session corp]\nfoo=bar\n[services local]\n")
	mustWrite(t, credentials, "[default]\nx=y\n[legacy]\nx=y\n[work]\nx=y\n")
	got, err := Profiles(config, credentials)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"default", "legacy", "work"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestClassifyAccessDenied(t *testing.T) {
	err := &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"}
	var source *SourceError
	if !errors.As(Classify(err, "describe table", "p"), &source) || source.Kind != ErrorAccessDenied {
		t.Fatalf("unexpected: %#v", source)
	}
}

func TestSourceErrorDoesNotExposeCredentialProcessOutput(t *testing.T) {
	err := Classify(errors.New("credential_process printed SECRET-VALUE"), "load", "work")
	if got := err.Error(); got != "load: configuration" {
		t.Fatalf("unsafe error: %q", got)
	}
}

func mustWrite(t *testing.T, p, s string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
}
