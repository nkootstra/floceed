package compose

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/nkootstra/floceed/internal/config"
	"go.yaml.in/yaml/v3"
)

func testProject() config.Project {
	return config.Project{SchemaVersion: 1, Source: config.Source{Region: "eu-west-1"}, Target: config.Target{FlociVersion: "1.6.0", Port: 4566, HookTimeoutSeconds: 300}}
}

func decode(t *testing.T, out []byte) *composeProject {
	t.Helper()
	var doc composeProject
	if err := yaml.Unmarshal(out, &doc); err != nil {
		t.Fatalf("Render output is not valid YAML: %v\n%s", err, out)
	}
	if len(doc.Services) != 1 {
		t.Fatalf("services = %d, want exactly floci:\n%s", len(doc.Services), out)
	}
	return &doc
}

func TestRenderPinsImageAndReadOnlyMounts(t *testing.T) {
	b, err := Render(testProject())
	if err != nil {
		t.Fatal(err)
	}
	doc := decode(t, b)
	floci := doc.Services["floci"]
	if floci.Image != Image {
		t.Errorf("image = %q, want %q", floci.Image, Image)
	}
	if len(floci.Ports) != 1 || floci.Ports[0] != "127.0.0.1:4566:4566" {
		t.Errorf("ports = %v, want [127.0.0.1:4566:4566]", floci.Ports)
	}
	if floci.Environment.HookTimeout != "300" {
		t.Errorf("hook timeout env = %q, want %q", floci.Environment.HookTimeout, "300")
	}
	if floci.Environment.StorageMode != "" || floci.Environment.StoragePath != "" {
		t.Errorf("unexpected persistence env = %#v", floci.Environment)
	}
	if len(floci.Volumes) != 2 {
		t.Fatalf("volumes = %#v", floci.Volumes)
	}
	want := []mount{
		{Type: "bind", Source: "./init/ready.d", Target: "/etc/floci/init/ready.d", ReadOnly: true, Bind: &bindOptions{CreateHostPath: false}},
		{Type: "bind", Source: "./", Target: "/floceed", ReadOnly: true, Bind: &bindOptions{CreateHostPath: false}},
	}
	for i, expected := range want {
		if !reflect.DeepEqual(floci.Volumes[i], expected) {
			t.Errorf("volume %d = %#v, want %#v", i, floci.Volumes[i], expected)
		}
	}
	if doc.Volumes != nil {
		t.Errorf("unexpected top-level volumes: %#v", doc.Volumes)
	}
	if strings.Contains(string(b), "\nname:") || strings.HasPrefix(string(b), "name:") {
		t.Errorf("compose output must rely on project-directory scoping, got hard-coded name in:\n%s", b)
	}
}

func TestRenderConfiguresDocumentedPersistenceVolume(t *testing.T) {
	p := testProject()
	p.Target.Persistence = config.Persistence{Enabled: true, Volume: "floceed-example"}
	b, err := Render(p)
	if err != nil {
		t.Fatal(err)
	}
	doc := decode(t, b)
	floci := doc.Services["floci"]
	if floci.Environment.StorageMode != "persistent" || floci.Environment.StoragePath != "/app/data" {
		t.Errorf("persistence env = %#v", floci.Environment)
	}
	if len(floci.Volumes) != 3 {
		t.Fatalf("volumes = %#v", floci.Volumes)
	}
	last := floci.Volumes[2]
	if last.Type != "volume" || last.Source != "floceed-example" || last.Target != "/app/data" || last.Bind != nil {
		t.Errorf("last mount = %#v", last)
	}
	if _, ok := doc.Volumes["floceed-example"]; !ok {
		t.Errorf("top-level volumes = %#v", doc.Volumes)
	}
	if strings.Contains(string(b), "/var/lib/floci") {
		t.Fatalf("obsolete persistence path in:\n%s", b)
	}
}

func TestRenderPersistenceBindMount(t *testing.T) {
	p := testProject()
	p.Target.Persistence = config.Persistence{Enabled: true, Path: "state/data"}
	b, err := Render(p)
	if err != nil {
		t.Fatal(err)
	}
	doc := decode(t, b)
	floci := doc.Services["floci"]
	if len(floci.Volumes) != 3 {
		t.Fatalf("volumes = %#v", floci.Volumes)
	}
	want := mount{Type: "bind", Source: "../state/data", Target: "/app/data", Bind: &bindOptions{CreateHostPath: true}}
	if !reflect.DeepEqual(floci.Volumes[2], want) {
		t.Errorf("last mount = %#v, want %#v", floci.Volumes[2], want)
	}
	if doc.Volumes != nil {
		t.Errorf("unexpected top-level volumes: %#v", doc.Volumes)
	}
}

func TestRenderIsDeterministic(t *testing.T) {
	p := testProject()
	p.Target.Persistence = config.Persistence{Enabled: true, Volume: "floceed-data"}
	first, err := Render(p)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		next, err := Render(p)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, next) {
			t.Fatalf("Render output changed between runs:\n%s\n---\n%s", first, next)
		}
	}
}

func TestRenderRejectsUnsafePersistence(t *testing.T) {
	t.Run("parent path", func(t *testing.T) {
		p := testProject()
		p.Target.Persistence = config.Persistence{Enabled: true, Path: "../escape"}
		if _, err := Render(p); err == nil {
			t.Fatal("Render accepted a path escaping the project directory")
		}
	})
	t.Run("invalid volume name", func(t *testing.T) {
		p := testProject()
		p.Target.Persistence = config.Persistence{Enabled: true, Volume: "bad name!"}
		if _, err := Render(p); err == nil {
			t.Fatal("Render accepted an invalid volume name")
		}
	})
}
