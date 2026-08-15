package compose

import (
	"strings"
	"testing"

	"github.com/nkootstra/floceed/internal/config"
)

func TestRenderPinsImageAndReadOnlyMounts(t *testing.T) {
	p := config.Project{SchemaVersion: 1, Source: config.Source{Region: "eu-west-1"}, Target: config.Target{FlociVersion: "1.6.0", Port: 4566, HookTimeoutSeconds: 300}}
	b, err := Render(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{Image, `"127.0.0.1:4566:4566"`, "create_host_path: false", "read_only: true", "/etc/floci/init/ready.d"} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}
	if strings.Contains(s, "\nname:") || strings.HasPrefix(s, "name:") {
		t.Errorf("compose output must rely on project-directory scoping, got hard-coded name in:\n%s", s)
	}
}

func TestRenderConfiguresDocumentedPersistencePath(t *testing.T) {
	p := config.Project{SchemaVersion: 1, Source: config.Source{Region: "eu-west-1"}, Target: config.Target{
		FlociVersion: "1.6.0", Port: 4566, HookTimeoutSeconds: 300,
		Persistence: config.Persistence{Enabled: true, Volume: "floceed-example"},
	}}
	b, err := Render(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`FLOCI_STORAGE_MODE: "persistent"`,
		`FLOCI_STORAGE_PERSISTENT_PATH: "/app/data"`,
		`target: /app/data`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in:\n%s", want, s)
		}
	}
	if strings.Contains(s, "/var/lib/floci") {
		t.Fatalf("obsolete persistence path in:\n%s", s)
	}
}
