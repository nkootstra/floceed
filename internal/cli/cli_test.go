package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/app"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func TestRootHelpUsesFloceedName(t *testing.T) {
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &out})
	cmd.SetArgs([]string{"--help"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "floceed") {
		t.Fatalf("help did not name floceed: %s", out.String())
	}
}

type fakeService struct {
	planned   bool
	pulled    bool
	rendered  bool
	doctored  bool
	started   bool
	wait      time.Duration
	doctor    app.DoctorResult
	doctorErr error
}

func (f *fakeService) Scan(context.Context, app.ScanRequest) (app.ScanResult, error) {
	return app.ScanResult{}, nil
}
func (f *fakeService) Plan(context.Context, config.Project, string, string) (app.Plan, error) {
	f.planned = true
	return app.Plan{Source: model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"}}, nil
}
func (f *fakeService) Pull(context.Context, config.Project, string, string, string) (model.Manifest, error) {
	f.pulled = true
	return model.Manifest{SchemaVersion: 1}, nil
}

func (f *fakeService) Render(context.Context, config.Project, string) (model.Manifest, error) {
	f.rendered = true
	return model.Manifest{SchemaVersion: 1}, nil
}
func (f *fakeService) Doctor(context.Context, config.Project, string, string, string) (app.DoctorResult, error) {
	f.doctored = true
	return f.doctor, f.doctorErr
}

func TestDoctorJSONFailureDoesNotEmitSuccessEnvelope(t *testing.T) {
	fake := &fakeService{
		doctor: app.DoctorResult{Checks: []app.Check{
			{Name: "project", OK: true, Message: "configuration is valid"},
			{Name: "docker", OK: false, Message: "Docker is unavailable"},
		}},
		doctorErr: &app.Error{Kind: app.ErrorLocal, Code: "DOCTOR_FAILED", Message: "one or more prerequisite checks failed"},
	}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"doctor", "--project", writeProject(t), "--output", "json"})

	err := cmd.ExecuteContext(context.Background())
	if err == nil || ExitCode(err) != 7 {
		t.Fatalf("got %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("doctor emitted output before returning its error: %s", out.String())
	}
	written, writeErr := WriteInvocationError(cmd, err)
	if !written || writeErr != nil {
		t.Fatalf("WriteInvocationError() = (%t, %v)", written, writeErr)
	}
	var envelope Envelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	result, ok := envelope.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v, want doctor result", envelope.Data)
	}
	checks, ok := result["checks"].([]any)
	if !ok || len(checks) != 2 {
		t.Fatalf("checks = %#v, want successful and failed checks", result["checks"])
	}
	wantChecks := []map[string]any{
		{"name": "project", "ok": true, "message": "configuration is valid"},
		{"name": "docker", "ok": false, "message": "Docker is unavailable"},
	}
	for i, want := range wantChecks {
		got, ok := checks[i].(map[string]any)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("check %d = %#v, want %#v", i, checks[i], want)
		}
	}
	if envelope.Status != StatusError || envelope.Error == nil || envelope.Error.Code != "DOCTOR_FAILED" {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}
}

func TestDoctorTextFailurePrintsChecksBeforeReturningError(t *testing.T) {
	fake := &fakeService{
		doctor:    app.DoctorResult{Checks: []app.Check{{Name: "aws", OK: false, Message: "credentials unavailable"}}},
		doctorErr: &app.Error{Kind: app.ErrorLocal, Code: "DOCTOR_FAILED", Message: "one or more prerequisite checks failed"},
	}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"doctor", "--project", writeProject(t)})

	err := cmd.ExecuteContext(context.Background())
	if err == nil || ExitCode(err) != 7 {
		t.Fatalf("got %v", err)
	}
	if got := out.String(); !strings.Contains(got, `"name": "aws"`) || !strings.Contains(got, `"ok": false`) {
		t.Fatalf("doctor output omitted checks: %s", got)
	}
}
func (f *fakeService) Up(_ context.Context, _ config.Project, _ string, wait time.Duration) error {
	f.started = true
	f.wait = wait
	return nil
}

func writeProject(t *testing.T) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "floceed.yaml")
	if err := os.WriteFile(name, []byte("schema_version: 1\nsource:\n  region: eu-west-1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestPlanHasRealProjectAndOutputFlags(t *testing.T) {
	fake := &fakeService{}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &out, App: fake})
	cmd.SetArgs([]string{"plan", "--project", writeProject(t), "--output", "json", "--profile", "dev", "--region", "eu-west-1"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fake.planned {
		t.Fatal("plan service not called")
	}
	if !strings.Contains(out.String(), `"command":"plan"`) {
		t.Fatalf("unexpected output %s", out.String())
	}
}

func TestPullRequiresYesWhenNonInteractive(t *testing.T) {
	fake := &fakeService{}
	cmd := New(Options{Stdin: strings.NewReader(""), Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"pull", "--project", writeProject(t)})
	err := cmd.ExecuteContext(context.Background())
	if err == nil || ExitCode(err) != 2 {
		t.Fatalf("got %v", err)
	}
	if fake.pulled {
		t.Fatal("pull ran without confirmation")
	}
}

func TestProjectCommandsExposeOnlyTheirOwnFlags(t *testing.T) {
	tests := []struct {
		name    string
		present []string
		absent  []string
	}{
		{name: "plan", present: []string{"project", "output", "profile", "region"}, absent: []string{"yes", "wait"}},
		{name: "pull", present: []string{"project", "output", "profile", "region", "yes"}, absent: []string{"wait"}},
		{name: "render", present: []string{"project", "output"}, absent: []string{"profile", "region", "yes", "wait"}},
		{name: "doctor", present: []string{"project", "output", "profile", "region"}, absent: []string{"yes", "wait"}},
		{name: "up", present: []string{"project", "output", "wait"}, absent: []string{"profile", "region", "yes"}},
	}

	root := New(Options{App: &fakeService{}})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, _, err := root.Find([]string{test.name})
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range test.present {
				if command.Flags().Lookup(name) == nil {
					t.Errorf("expected --%s", name)
				}
			}
			for _, name := range test.absent {
				if command.Flags().Lookup(name) != nil {
					t.Errorf("did not expect --%s", name)
				}
			}
		})
	}
}

func TestRenderDoctorAndUpDispatchToTheirServices(t *testing.T) {
	fake := &fakeService{}
	project := writeProject(t)
	for _, args := range [][]string{
		{"render", "--project", project},
		{"doctor", "--project", project},
		{"up", "--project", project, "--wait", "3s"},
	} {
		cmd := New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, App: fake})
		cmd.SetArgs(args)
		if err := cmd.ExecuteContext(context.Background()); err != nil {
			t.Fatalf("%s: %v", args[0], err)
		}
	}
	if !fake.rendered || !fake.doctored || !fake.started {
		t.Fatalf("dispatch state: rendered=%t doctored=%t started=%t", fake.rendered, fake.doctored, fake.started)
	}
	if fake.wait != 3*time.Second {
		t.Fatalf("up wait = %s, want 3s", fake.wait)
	}
}

func TestJSONEnvelope(t *testing.T) {
	b, err := json.Marshal(Envelope{SchemaVersion: 1, Command: "scan", Status: StatusSuccess})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "data") || !strings.Contains(string(b), `"status":"success"`) {
		t.Fatalf("unexpected JSON: %s", b)
	}
}

func TestCancellationExitCode(t *testing.T) {
	if got := ExitCode(context.Canceled); got != 130 {
		t.Fatalf("got %d, want 130", got)
	}
}
