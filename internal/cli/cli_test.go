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
	inspection "github.com/nkootstra/floceed/internal/inspect"
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

func TestRootExposesFixtureProfileForInteractiveMode(t *testing.T) {
	cmd := New(Options{})
	flag := cmd.Flags().Lookup("fixture-profile")
	if flag == nil {
		t.Fatal("root command does not expose --fixture-profile")
	}
	if err := flag.Value.Set("share-safe"); err != nil {
		t.Fatal(err)
	}
	if got := flag.Value.String(); got != "share-safe" {
		t.Fatalf("fixture profile = %q, want share-safe", got)
	}
}

type fakeService struct {
	planned        bool
	pulled         bool
	rendered       bool
	doctored       bool
	started        bool
	wait           time.Duration
	doctor         app.DoctorResult
	doctorErr      error
	fixtureProfile string
	inspectResult  inspection.Inspection
	inspectErr     error
	inspectOptions app.InspectOptions
	inspected      bool
}

func (f *fakeService) InspectWithOptions(_ context.Context, _ config.Project, _ string, options app.InspectOptions) (inspection.Inspection, error) {
	f.inspected = true
	f.inspectOptions = options
	return f.inspectResult, f.inspectErr
}

func TestInspectJSONUsesOneStableEnvelopeAndForwardsOptions(t *testing.T) {
	fake := &fakeService{inspectResult: inspection.Inspection{SchemaVersion: 1, Valid: true, ManifestSchema: 3, BundleIdentity: "sha256:current", Runtime: inspection.Runtime{State: inspection.RuntimeUnavailable, Diagnostic: "connection refused"}}}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"inspect", "--project", writeProject(t), "--output", "json", "--compare", "baseline", "--runtime"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&out)
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if decoder.Decode(&struct{}{}) == nil {
		t.Fatal("inspect emitted more than one JSON value")
	}
	if envelope.SchemaVersion != 1 || envelope.Command != "inspect" || envelope.Status != StatusSuccess {
		t.Fatalf("envelope = %#v", envelope)
	}
	if !fake.inspected || fake.inspectOptions.ComparePath != "baseline" || !fake.inspectOptions.Runtime {
		t.Fatalf("inspect options = %#v", fake.inspectOptions)
	}
}

func TestInspectTextIsConciseDeterministicAndHasNoANSI(t *testing.T) {
	fake := &fakeService{inspectResult: inspection.Inspection{
		SchemaVersion: 1, Valid: true, ManifestSchema: 3, BundleIdentity: "sha256:current", SelectedResources: 1,
		Source: inspection.SourceProjection{AccountID: "123456789012", Region: "eu-west-1"},
		Target: inspection.TargetProjection{FlociVersion: "1.6.0"}, Artifacts: inspection.ArtifactSummary{Files: 2, Bytes: 42},
		Services:  []inspection.ServiceSummary{{Service: "s3", Resources: 1, Selected: 1, Records: 2, SourceBytes: 21}},
		Resources: []inspection.Resource{{Identity: inspection.ResourceIdentity{Service: "s3", Type: "bucket", ID: "assets"}, Selected: true}},
		Runtime:   inspection.Runtime{State: inspection.RuntimeNotRequested},
		Receipt:   &inspection.Receipt{SchemaVersion: 1, Baseline: "sha256:baseline", Current: "sha256:current", Counts: inspection.ReceiptCounts{Changed: 1}, Resources: []inspection.ResourceChange{{Resource: inspection.ResourceIdentity{Service: "s3", Type: "bucket", ID: "assets"}, Outcome: inspection.OutcomeChanged, Categories: []inspection.ChangeCategory{inspection.CategoryDataset}}}},
	}}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"inspect", "--project", writeProject(t), "--no-color"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := "Bundle: valid\nIdentity: sha256:current\nManifest schema: 3\nSource: 123456789012 / eu-west-1\nTarget: Floci 1.6.0\nResources: 1 selected\nArtifacts: 2 files, 42 bytes\nRuntime: not requested\n\nServices\ns3: 1 resources, 1 selected, 2 records, 21 bytes\n\nResources\ns3/bucket/assets: selected\n\nComparison\nBaseline: sha256:baseline\nCurrent: sha256:current\nChanges: 0 added, 0 removed, 1 changed, 0 unchanged\ns3/bucket/assets: changed (dataset)\n"
	if out.String() != want {
		t.Fatalf("text output:\n%s\nwant:\n%s", out.String(), want)
	}
	if strings.Contains(out.String(), "\x1b[") {
		t.Fatalf("text contains ANSI: %q", out.String())
	}
}

func TestInspectJSONInvalidBundleUsesStableErrorEnvelopeAndExitCode(t *testing.T) {
	fake := &fakeService{inspectErr: &app.Error{Kind: app.ErrorFilesystem, Code: "BUNDLE_INTEGRITY_INVALID", Message: "bundle inspection failed", Remediation: "Regenerate the bundle."}}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"inspect", "--project", writeProject(t), "--output", "json"})
	err := cmd.ExecuteContext(context.Background())
	if err == nil || ExitCode(err) != 6 || out.Len() != 0 {
		t.Fatalf("Execute() = %v, output %q", err, out.String())
	}
	written, writeErr := WriteInvocationError(cmd, err)
	if !written || writeErr != nil {
		t.Fatalf("WriteInvocationError() = %t, %v", written, writeErr)
	}
	var envelope Envelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Command != "inspect" || envelope.Status != StatusError || envelope.Error == nil || envelope.Error.Code != "BUNDLE_INTEGRITY_INVALID" {
		t.Fatalf("envelope = %#v", envelope)
	}
}

func TestInspectCommittedFixturesOffline(t *testing.T) {
	current := filepath.Join("testdata", "inspect", "current", "floceed.yaml")
	baseline := filepath.Join("testdata", "inspect", "baseline", "floceed.yaml")
	governanceCurrent := filepath.Join("testdata", "inspect", "governance-current", "floceed.yaml")
	for _, args := range [][]string{
		{"inspect", "--project", current},
		{"inspect", "--project", current, "--output", "json"},
		{"inspect", "--project", current, "--compare", baseline, "--output", "json"},
		{"inspect", "--project", governanceCurrent, "--compare", current, "--output", "json"},
	} {
		var out bytes.Buffer
		cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: app.New("test")})
		cmd.SetArgs(args)
		if err := cmd.ExecuteContext(context.Background()); err != nil {
			t.Fatalf("floceed %v: %v", args, err)
		}
		for _, canary := range []string{"FIXTURE_RECORD_SECRET_CANARY", "GOVERNANCE_REPLACEMENT_SECRET_CANARY"} {
			if strings.Contains(out.String(), canary) {
				t.Fatalf("floceed %v disclosed %q", args, canary)
			}
		}
	}

	result := inspectFixtureJSON(t, current, baseline)
	if result.Receipt == nil || result.Receipt.Counts != (inspection.ReceiptCounts{Added: 1, Removed: 1, Changed: 1, Unchanged: 1}) {
		t.Fatalf("baseline receipt = %#v", result.Receipt)
	}
	governance := inspectFixtureJSON(t, governanceCurrent, current)
	if governance.Receipt == nil || governance.Receipt.Counts.Changed == 0 {
		t.Fatalf("governance receipt = %#v", governance.Receipt)
	}
	for _, change := range governance.Receipt.Resources {
		if change.Outcome == inspection.OutcomeChanged && !reflect.DeepEqual(change.Categories, []inspection.ChangeCategory{inspection.CategoryGovernance}) {
			t.Fatalf("governance categories = %v", change.Categories)
		}
	}
}

func inspectFixtureJSON(t *testing.T, project, compare string) inspection.Inspection {
	t.Helper()
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: app.New("test")})
	cmd.SetArgs([]string{"inspect", "--project", project, "--compare", compare, "--output", "json"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Data inspection.Inspection `json:"data"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Data
}

func (f *fakeService) PlanWithOptions(_ context.Context, _ config.Project, options app.PlanOptions) (app.Plan, error) {
	f.planned = true
	f.fixtureProfile = options.FixtureProfile
	return app.Plan{}, nil
}

func (f *fakeService) PullWithOptions(_ context.Context, _ config.Project, _ string, _ string, _ string, options app.PullOptions) (app.PullResult, error) {
	f.pulled = true
	f.fixtureProfile = options.FixtureProfile
	return app.PullResult{Manifest: model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion}, Baseline: app.BaselineAbsent}, nil
}

type progressService struct{ fakeService }

func (f *progressService) PullWithOptions(_ context.Context, _ config.Project, _ string, _ string, _ string, options app.PullOptions) (app.PullResult, error) {
	options.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "dynamodb", Resource: "orders", CompletedRecords: 5, TotalRecords: 10, TotalPrecision: "estimated"})
	return app.PullResult{Manifest: model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion}, Baseline: app.BaselineAbsent}, nil
}

func TestPullJSONProgressUsesStderrWithoutCorruptingFinalEnvelope(t *testing.T) {
	service := &progressService{}
	var stdout, stderr bytes.Buffer
	cmd := New(Options{Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr, App: service})
	cmd.SetArgs([]string{"pull", "--project", writeProject(t), "--yes", "--output", "json", "--progress", "json", "--work-dir", t.TempDir()})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var envelope Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("stdout is not one JSON envelope: %q: %v", stdout.String(), err)
	}
	if envelope.Command != "pull" || envelope.Status != StatusSuccess {
		t.Fatalf("pull envelope = %#v", envelope)
	}
	payload, ok := envelope.Data.(map[string]any)
	if !ok || payload["baseline"] != string(app.BaselineAbsent) || payload["manifest"] == nil {
		t.Fatalf("pull payload = %#v", envelope.Data)
	}
	if strings.Contains(stdout.String(), "secret-canary") {
		t.Fatalf("pull output disclosed privacy canary: %s", stdout.String())
	}
	var event model.ProgressEvent
	if err := json.Unmarshal(stderr.Bytes(), &event); err != nil {
		t.Fatalf("stderr progress is not JSON: %q: %v", stderr.String(), err)
	}
	if event.Resource != "orders" || event.TotalPrecision != "estimated" {
		t.Fatalf("event = %#v", event)
	}
}

func TestPullPlainProgressMarksEstimatesAndOmitsEmptyFields(t *testing.T) {
	service := &progressService{}
	var stdout, stderr bytes.Buffer
	cmd := New(Options{Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr, App: service})
	cmd.SetArgs([]string{"pull", "--project", writeProject(t), "--yes", "--progress", "plain", "--work-dir", t.TempDir()})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(stderr.String())
	if line != "capture dynamodb orders 5/~10" {
		t.Fatalf("plain progress = %q", line)
	}
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

func TestPlanForwardsFixtureProfileSeparatelyFromAWSProfile(t *testing.T) {
	fake := &fakeService{}
	cmd := New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"plan", "--project", writeProject(t), "--profile", "aws-dev", "--fixture-profile", "share-safe"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.fixtureProfile != "share-safe" {
		t.Fatalf("fixture profile = %q, want share-safe", fake.fixtureProfile)
	}
}

func TestPullForwardsFixtureProfile(t *testing.T) {
	fake := &fakeService{}
	cmd := New(Options{Stdin: strings.NewReader(""), Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"pull", "--project", writeProject(t), "--yes", "--fixture-profile", "share-safe"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.fixtureProfile != "share-safe" {
		t.Fatalf("fixture profile = %q, want share-safe", fake.fixtureProfile)
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
		{name: "plan", present: []string{"project", "output", "profile", "region", "fixture-profile"}, absent: []string{"yes", "wait"}},
		{name: "pull", present: []string{"project", "output", "profile", "region", "fixture-profile", "yes"}, absent: []string{"wait"}},
		{name: "render", present: []string{"project", "output"}, absent: []string{"profile", "region", "yes", "wait"}},
		{name: "doctor", present: []string{"project", "output", "profile", "region"}, absent: []string{"yes", "wait"}},
		{name: "up", present: []string{"project", "output", "wait"}, absent: []string{"profile", "region", "yes"}},
		{name: "inspect", present: []string{"project", "output", "compare", "runtime"}, absent: []string{"profile", "region", "yes", "wait", "fixture-profile"}},
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
