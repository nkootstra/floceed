package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/config"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/nkootstra/floceed/internal/model"
)

type panicSourceFactory struct{}

func (panicSourceFactory) Open(context.Context, SourceRequest) (Source, error) {
	panic("inspect must not open an AWS source")
}

type panicInspectRuntime struct{}

func (panicInspectRuntime) DoctorChecks(context.Context) []Check {
	panic("inspect must not call Docker")
}
func (panicInspectRuntime) Start(context.Context, string, string) ([]byte, error) {
	panic("inspect must not call Docker")
}
func (panicInspectRuntime) WaitReady(context.Context, string, time.Duration) error {
	panic("inspect must not call runtime readiness")
}
func (panicInspectRuntime) InspectStatus(context.Context, string, time.Duration) (inspection.Runtime, error) {
	panic("inspect must not call runtime status")
}

type inspectRuntimeStub struct {
	result inspection.Runtime
	err    error
	calls  int
	url    string
}

func (r *inspectRuntimeStub) DoctorChecks(context.Context) []Check                   { return nil }
func (r *inspectRuntimeStub) Start(context.Context, string, string) ([]byte, error)  { return nil, nil }
func (r *inspectRuntimeStub) WaitReady(context.Context, string, time.Duration) error { return nil }
func (r *inspectRuntimeStub) InspectStatus(_ context.Context, url string, _ time.Duration) (inspection.Runtime, error) {
	r.calls++
	r.url = url
	return r.result, r.err
}

func TestInspectReadsCustomOutputWithoutOpeningSource(t *testing.T) {
	projectDir := t.TempDir()
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.Output.Directory = "generated/local"
	root := filepath.Join(projectDir, "generated", "local")
	manifest := model.Manifest{
		SchemaVersion: 3,
		Tool:          model.ToolMetadata{Version: "v0.2.0"},
		Source:        model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"},
		Selected:      []model.ResourceRef{{Service: "s3", Type: "bucket", ID: "assets"}},
		Snapshots: []model.Snapshot{{
			Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}, Service: "s3",
			StructureVersion: 1, Structure: json.RawMessage(`{"name":"assets","region":"eu-west-1"}`),
		}},
		Operations: []model.Operation{{ID: "create-s3-assets", Stage: model.StageBase, Service: "s3", ResourceID: "assets", Action: "create"}},
	}
	writeInspectableBundle(t, root, manifest)
	service := New("test")
	service.Factory = panicSourceFactory{}
	service.localRuntime = panicInspectRuntime{}

	got, err := service.Inspect(context.Background(), project, projectDir)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Valid || got.ManifestSchema != 3 || got.BundleIdentity == "" || got.SelectedResources != 1 {
		t.Fatalf("Inspect() = %#v", got)
	}
	if len(got.Resources) != 1 || got.Resources[0].Identity.ID != "assets" {
		t.Fatalf("resources = %#v", got.Resources)
	}
	if got.Artifacts.Files != 4 || got.Artifacts.Bytes == 0 || len(got.Services) != 1 || got.Services[0].Selected != 1 || len(got.Operations) != 1 {
		t.Fatalf("inspection summaries = artifacts %#v, services %#v, operations %#v", got.Artifacts, got.Services, got.Operations)
	}
}

func TestInspectRuntimeIsOptionalAndAdditive(t *testing.T) {
	projectDir := t.TempDir()
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.Target.Port = 4567
	writeInspectableBundle(t, filepath.Join(projectDir, project.Output.Directory), comparableManifest(t, "current"))
	runtime := &inspectRuntimeStub{result: inspection.Runtime{State: inspection.RuntimeUnavailable, Diagnostic: "connection refused"}}
	service := New("test")
	service.localRuntime = runtime

	without, err := service.Inspect(context.Background(), project, projectDir)
	if err != nil || runtime.calls != 0 || without.Runtime.State != inspection.RuntimeNotRequested {
		t.Fatalf("default inspect = %#v, %v; calls=%d", without, err, runtime.calls)
	}
	with, err := service.InspectWithOptions(context.Background(), project, projectDir, InspectOptions{Runtime: true})
	if err != nil || !with.Valid || with.Runtime.State != inspection.RuntimeUnavailable || runtime.calls != 1 {
		t.Fatalf("runtime inspect = %#v, %v; calls=%d", with, err, runtime.calls)
	}
	if runtime.url != "http://127.0.0.1:4567/_floci/init" {
		t.Fatalf("runtime url = %q", runtime.url)
	}
}

func TestRuntimeStatusClassifiesReadinessAndBoundsDiagnostics(t *testing.T) {
	tests := []struct {
		name   string
		do     httpDoerFunc
		want   inspection.RuntimeState
		failed []string
	}{
		{name: "ready", do: runtimeResponse(200, `{"completed":{"ready":true},"scripts":{"ready":[]}}`), want: inspection.RuntimeReady},
		{name: "not ready", do: runtimeResponse(200, `{"completed":{"ready":false},"scripts":{"ready":[]}}`), want: inspection.RuntimeNotReady},
		{name: "failed scripts", do: runtimeResponse(200, `{"completed":{"ready":false},"scripts":{"ready":[{"script":"zeta","state":"FAILED"},{"script":"alpha","return_code":1}]}}`), want: inspection.RuntimeNotReady, failed: []string{"alpha", "zeta"}},
		{name: "connection refused", do: func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp: connection refused\nsecret trailing line")
		}, want: inspection.RuntimeUnavailable},
		{name: "timeout", do: func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}, want: inspection.RuntimeUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := newDockerLocalRuntime()
			runtime.httpClient = test.do
			got, err := runtime.InspectStatus(context.Background(), "http://127.0.0.1/_floci/init", time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != test.want || (test.failed != nil && !reflect.DeepEqual(got.FailedScripts, test.failed)) || (test.failed == nil && len(got.FailedScripts) != 0) {
				t.Fatalf("status = %#v, want %s / %v", got, test.want, test.failed)
			}
			if len(got.Diagnostic) > 160 || strings.ContainsAny(got.Diagnostic, "\r\n\t") {
				t.Fatalf("diagnostic is not bounded/sanitized: %q", got.Diagnostic)
			}
		})
	}
}

func TestInspectRuntimePropagatesParentCancellation(t *testing.T) {
	projectDir := t.TempDir()
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	writeInspectableBundle(t, filepath.Join(projectDir, project.Output.Directory), comparableManifest(t, "current"))
	runtime := &inspectRuntimeStub{err: context.Canceled}
	service := New("test")
	service.localRuntime = runtime

	got, err := service.InspectWithOptions(context.Background(), project, projectDir, InspectOptions{Runtime: true})
	var appErr *Error
	if !errors.As(err, &appErr) || appErr.Code != "INSPECTION_CANCELED" || !errors.Is(err, context.Canceled) {
		t.Fatalf("InspectWithOptions() = %#v, %v; want typed cancellation", got, err)
	}
	if got.Valid {
		t.Fatalf("canceled runtime inspection returned valid result: %#v", got)
	}
}

func TestRuntimeStatusPropagatesParentDeadlineButOwnTimeoutIsUnavailable(t *testing.T) {
	runtime := newDockerLocalRuntime()
	runtime.httpClient = httpDoerFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runtime.InspectStatus(parent, "http://127.0.0.1/_floci/init", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation error = %v", err)
	}

	got, err := runtime.InspectStatus(context.Background(), "http://127.0.0.1/_floci/init", time.Millisecond)
	if err != nil || got.State != inspection.RuntimeUnavailable {
		t.Fatalf("owned timeout = %#v, %v; want unavailable success", got, err)
	}
}

func runtimeResponse(status int, body string) httpDoerFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

func TestInspectMapsInvalidArtifactsToStableErrorsAndNeverReturnsValid(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string)
		code  string
	}{
		{"missing bundle", func(*testing.T, string) {}, "BUNDLE_NOT_FOUND"},
		{"checksum mismatch", func(t *testing.T, root string) {
			writeInspectableBundle(t, root, model.Manifest{SchemaVersion: 1})
			os.WriteFile(filepath.Join(root, "bundle", "manifest.json"), []byte("changed"), 0o600)
		}, "BUNDLE_INTEGRITY_INVALID"},
		{"unsupported manifest", func(t *testing.T, root string) {
			writeInspectableBundle(t, root, model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion + 1})
		}, "MANIFEST_SCHEMA_UNSUPPORTED"},
		{"unsafe checksum path", func(t *testing.T, root string) {
			writeInspectableBundle(t, root, model.Manifest{SchemaVersion: 1})
			index, _ := bundle.CanonicalJSON(bundle.Checksums{SchemaVersion: 1, Files: []bundle.Checksum{{Path: "../escape", SHA256: strings.Repeat("0", 64)}}})
			os.WriteFile(filepath.Join(root, "checksums.json"), index, 0o600)
		}, "BUNDLE_PATH_UNSAFE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projectDir := t.TempDir()
			project := config.NewProject()
			project.Source.Region = "eu-west-1"
			root := filepath.Join(projectDir, project.Output.Directory)
			test.setup(t, root)
			got, err := New("test").Inspect(context.Background(), project, projectDir)
			var appErr *Error
			if !errors.As(err, &appErr) || appErr.Code != test.code || appErr.Remediation == "" {
				t.Fatalf("Inspect() error = %#v, want code %s with remediation", err, test.code)
			}
			if got.Valid {
				t.Fatalf("Inspect() returned valid result for invalid artifact: %#v", got)
			}
		})
	}
}

func TestInspectHonorsCanceledContextBeforeExternalCapabilities(t *testing.T) {
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	service := New("test")
	service.Factory = panicSourceFactory{}
	service.localRuntime = panicInspectRuntime{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := service.Inspect(ctx, project, t.TempDir())
	var appErr *Error
	if !errors.As(err, &appErr) || appErr.Code != "INSPECTION_CANCELED" || !errors.Is(err, context.Canceled) {
		t.Fatalf("Inspect() = %#v, %v; want typed cancellation", got, err)
	}
	if got.Valid {
		t.Fatalf("canceled inspection returned valid: %#v", got)
	}
}

func TestInspectComparisonAcceptsGeneratedDirectoryAndProjectFile(t *testing.T) {
	currentDir := t.TempDir()
	currentProject := config.NewProject()
	currentProject.Source.Region = "eu-west-1"
	currentManifest := comparableManifest(t, "current")
	writeInspectableBundle(t, filepath.Join(currentDir, currentProject.Output.Directory), currentManifest)

	baselineDir := t.TempDir()
	baselineRoot := filepath.Join(baselineDir, "generated")
	baselineManifest := comparableManifest(t, "baseline")
	writeInspectableBundle(t, baselineRoot, baselineManifest)
	projectPath := filepath.Join(baselineDir, "floceed.yaml")
	projectYAML := "schema_version: 1\nsource:\n  region: eu-west-1\noutput:\n  directory: generated\n"
	if err := os.WriteFile(projectPath, []byte(projectYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	service := New("test")
	service.Factory = panicSourceFactory{}
	service.localRuntime = panicInspectRuntime{}
	for name, comparePath := range map[string]string{"generated directory": baselineRoot, "project file": projectPath} {
		t.Run(name, func(t *testing.T) {
			got, err := service.InspectWithOptions(context.Background(), currentProject, currentDir, InspectOptions{ComparePath: comparePath})
			if err != nil {
				t.Fatal(err)
			}
			if got.Receipt == nil || got.Receipt.Baseline == "" || got.Receipt.Current != got.BundleIdentity {
				t.Fatalf("comparison inspection = %#v", got)
			}
			if got.Receipt.Counts.Changed != 1 || got.Receipt.Resources[0].Categories[0] != "structure" {
				t.Fatalf("receipt = %#v", got.Receipt)
			}
		})
	}
}

func TestInspectComparisonRejectsInvalidTargetWithoutPartialResult(t *testing.T) {
	currentDir := t.TempDir()
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	writeInspectableBundle(t, filepath.Join(currentDir, project.Output.Directory), comparableManifest(t, "current"))

	tests := []struct{ name, path, code string }{
		{"missing", filepath.Join(t.TempDir(), "missing"), "COMPARE_TARGET_NOT_FOUND"},
		{"ambiguous file", filepath.Join(t.TempDir(), "target.txt"), "COMPARE_TARGET_AMBIGUOUS"},
	}
	if err := os.WriteFile(tests[1].path, []byte("not a project"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := New("test").InspectWithOptions(context.Background(), project, currentDir, InspectOptions{ComparePath: test.path})
			var appErr *Error
			if !errors.As(err, &appErr) || appErr.Code != test.code {
				t.Fatalf("error = %#v, want %s", err, test.code)
			}
			if got.Valid || got.Receipt != nil || got.BundleIdentity != "" {
				t.Fatalf("partial comparison result = %#v", got)
			}
		})
	}
}

func comparableManifest(t *testing.T, marker string) model.Manifest {
	t.Helper()
	snapshot, err := model.NewSnapshot(model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}, "s3", map[string]any{"name": "assets", "region": "eu-west-1", "marker": marker})
	if err != nil {
		t.Fatal(err)
	}
	return model.Manifest{SchemaVersion: 3, Source: model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"}, Selected: []model.ResourceRef{snapshot.Resource}, Snapshots: []model.Snapshot{*snapshot}, Operations: []model.Operation{{ID: "create", Stage: model.StageBase, Service: "s3", ResourceID: "assets", Action: "create"}}}
}

func writeInspectableBundle(t *testing.T, root string, manifest model.Manifest) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "bundle"), 0o700); err != nil {
		t.Fatal(err)
	}
	b, err := bundle.CanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "bundle", "manifest.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestSum, err := bundle.SumFile(path)
	if err != nil {
		t.Fatal(err)
	}
	manifestSum.Path = "bundle/manifest.json"
	sums := []bundle.Checksum{manifestSum}
	for _, artifact := range []struct{ name, contents string }{
		{bundle.ComposeFile, "services: {}\n"},
		{"runtime/replay.py", "# replay\n"},
		{"init/ready.d/10-replay.py", "# ready\n"},
	} {
		name, contents := artifact.name, artifact.contents
		artifactPath := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(artifactPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(artifactPath, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		sum, err := bundle.SumFile(artifactPath)
		if err != nil {
			t.Fatal(err)
		}
		sum.Path = name
		sums = append(sums, sum)
	}
	index, err := bundle.CanonicalJSON(bundle.Checksums{SchemaVersion: 1, Files: sums})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "checksums.json"), index, 0o600); err != nil {
		t.Fatal(err)
	}
}
