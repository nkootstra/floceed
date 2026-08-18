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

func TestInitCreatesMinimalProjectWithoutAWS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "floceed.yaml")
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}})
	cmd.SetArgs([]string{"init", "--project", path, "--region", "eu-west-1"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	project, err := config.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if project.Source.Region != "eu-west-1" || project.Target.Port != config.DefaultPort || !strings.Contains(out.String(), path) {
		t.Fatalf("project/output = %#v, %s", project, out.String())
	}
}

func TestInitRefusesOverwriteUnlessForced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "floceed.yaml")
	original := []byte("existing\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	cmd.SetArgs([]string{"init", "--project", path, "--region", "eu-west-1"})
	err := cmd.ExecuteContext(context.Background())
	if err == nil || ExitCode(err) != 6 || !strings.Contains(err.Error(), "file exists") {
		t.Fatalf("overwrite error = %v", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("existing project changed without --force")
	}
	cmd = New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	cmd.SetArgs([]string{"init", "--project", path, "--region", "eu-west-1", "--force"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, readErr = os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if bytes.Equal(got, original) {
		t.Fatal("--force did not replace project")
	}
}

func TestInitRequiresRegion(t *testing.T) {
	cmd := New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	cmd.SetArgs([]string{"init"})
	if err := cmd.ExecuteContext(context.Background()); err == nil || ExitCode(err) != 2 {
		t.Fatalf("region error = %v", err)
	}
}

func TestInitJSONUsesStableEnvelope(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "floceed.yaml")
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}})
	cmd.SetArgs([]string{"init", "--project", path, "--region", "eu-west-1", "--output", "json"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var envelope Envelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Command != "init" || envelope.Status != StatusSuccess {
		t.Fatalf("envelope = %#v", envelope)
	}
}

func TestInitReportsMissingParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "floceed.yaml")
	cmd := New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	cmd.SetArgs([]string{"init", "--project", path, "--region", "eu-west-1"})
	if err := cmd.ExecuteContext(context.Background()); err == nil || ExitCode(err) != 6 {
		t.Fatalf("parent error = %v", err)
	}
}

func TestCapabilitiesJSONIsOfflineAndStable(t *testing.T) {
	var out bytes.Buffer
	cmd := New(Options{Version: "v0.11.0", Stdout: &out, Stderr: &bytes.Buffer{}})
	cmd.SetArgs([]string{"capabilities", "--output", "json"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var envelope Envelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatalf("capabilities output is not JSON: %v", err)
	}
	if envelope.Command != "capabilities" || envelope.Status != StatusSuccess {
		t.Fatalf("envelope = %#v", envelope)
	}
	payload := envelope.Data.(map[string]any)
	if payload["tool_version"] != "v0.11.0" || payload["floci_version"] != "1.6.0" {
		t.Fatalf("capabilities payload = %#v", payload)
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
	logs           []byte
	downCalled     bool
	downDir        string
	downErr        error
	resetCalled    bool
}

func (f *fakeService) InspectWithOptions(_ context.Context, _ config.Project, _ string, options app.InspectOptions) (inspection.Inspection, error) {
	f.inspected = true
	f.inspectOptions = options
	return f.inspectResult, f.inspectErr
}

func (f *fakeService) Logs(context.Context, config.Project, string, int) ([]byte, error) {
	return f.logs, nil
}

func TestInspectJSONUsesOneStableEnvelopeAndForwardsOptions(t *testing.T) {
	fake := &fakeService{inspectResult: inspection.Inspection{SchemaVersion: 1, Valid: true, ManifestSchema: 3, BundleIdentity: "sha256:current", Runtime: inspection.Runtime{State: inspection.RuntimeUnavailable, Diagnostic: "connection refused"}}}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"inspect", "--project", writeProject(t), "--output", "json", "--compare", "baseline", "--runtime", "--artifacts"})
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
	if !fake.inspected || fake.inspectOptions.ComparePath != "baseline" || !fake.inspectOptions.Runtime || !fake.inspectOptions.Artifacts {
		t.Fatalf("inspect options = %#v", fake.inspectOptions)
	}
}

func TestStatusUsesRuntimeInspectionAndStableText(t *testing.T) {
	fake := &fakeService{inspectResult: inspection.Inspection{Runtime: inspection.Runtime{State: inspection.RuntimeReady}}}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"status", "--project", writeProject(t)})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fake.inspected || !fake.inspectOptions.Runtime || !strings.Contains(out.String(), "Bundle: valid") || !strings.Contains(out.String(), "Runtime: ready") {
		t.Fatalf("status output = %q, options = %#v", out.String(), fake.inspectOptions)
	}
}

func TestLogsForwardsTailAndWritesOutput(t *testing.T) {
	fake := &fakeService{logs: []byte("floci started\n")}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"logs", "--project", writeProject(t), "--tail", "25"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "floci started\n" {
		t.Fatalf("logs output = %q", got)
	}
}

func TestDownRequiresExplicitConfirmation(t *testing.T) {
	cmd := New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, App: &fakeService{}})
	cmd.SetArgs([]string{"down", "--project", writeProject(t)})
	if err := cmd.ExecuteContext(context.Background()); err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("down without confirmation error = %v", err)
	}
}

func TestDownDelegatesAfterConfirmation(t *testing.T) {
	fake := &fakeService{}
	var out bytes.Buffer
	projectFile := writeProject(t)
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"down", "--project", projectFile, "--yes"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fake.downCalled || fake.downDir != filepath.Dir(projectFile) || !strings.Contains(out.String(), "preserved_data") {
		t.Fatalf("down delegation/output: called=%v dir=%q output=%q", fake.downCalled, fake.downDir, out.String())
	}
}

func TestResetRequiresExplicitConfirmation(t *testing.T) {
	cmd := New(Options{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, App: &fakeService{}})
	cmd.SetArgs([]string{"reset", "--project", writeProject(t)})
	if err := cmd.ExecuteContext(context.Background()); err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("reset without confirmation error = %v", err)
	}
}

func TestInspectTextIsConciseDeterministicAndHasNoANSI(t *testing.T) {
	fake := &fakeService{inspectResult: inspection.Inspection{
		SchemaVersion: 1, Valid: true, ManifestSchema: 3, BundleIdentity: "sha256:current", SelectedResources: 1,
		Source: inspection.SourceProjection{AccountID: "123456789012", Region: "eu-west-1"},
		Target: inspection.TargetProjection{FlociVersion: "1.6.0"}, Artifacts: inspection.ArtifactSummary{Files: 2, Bytes: 42},
		Services:  []inspection.ServiceSummary{{Service: "s3", Resources: 1, Selected: 1, Records: 2, SourceBytes: 21}},
		Findings:  []inspection.Finding{{Code: "BUNDLE_WARNING", Severity: "warning", Support: "bundle support", Resource: "assets", Property: "versioning"}},
		Resources: []inspection.Resource{{Identity: inspection.ResourceIdentity{Service: "s3", Type: "bucket", ID: "assets"}, Selected: true, Findings: []inspection.Finding{{Code: "RESOURCE_WARNING", Severity: "warning", Support: "resource support", Resource: "assets", Property: "policy"}}}},
		Runtime:   inspection.Runtime{State: inspection.RuntimeNotRequested},
		Receipt:   &inspection.Receipt{SchemaVersion: 1, Baseline: "sha256:baseline", Current: "sha256:current", Categories: []inspection.ChangeCategory{inspection.CategoryDataset, inspection.CategoryFindings}, Counts: inspection.ReceiptCounts{Changed: 1}, Resources: []inspection.ResourceChange{{Resource: inspection.ResourceIdentity{Service: "s3", Type: "bucket", ID: "assets"}, Outcome: inspection.OutcomeChanged, Categories: []inspection.ChangeCategory{inspection.CategoryDataset}}}},
	}}
	var out bytes.Buffer
	cmd := New(Options{Stdout: &out, Stderr: &bytes.Buffer{}, App: fake})
	cmd.SetArgs([]string{"inspect", "--project", writeProject(t), "--no-color"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := "Bundle: valid\nIdentity: sha256:current\nManifest schema: 3\nSource: 123456789012 / eu-west-1\nTarget: Floci 1.6.0\nResources: 1 selected\nArtifacts: 2 files, 42 bytes\nRuntime: not requested\n\nFindings\nWARNING BUNDLE_WARNING: bundle support [resource=assets, property=versioning]\n\nServices\ns3: 1 resources, 1 selected, 2 records, 21 bytes\n\nResources\ns3/bucket/assets: selected\n  Findings\n  WARNING RESOURCE_WARNING: resource support [resource=assets, property=policy]\n\nComparison\nBaseline: sha256:baseline\nCurrent: sha256:current\nCategories: dataset, findings\nChanges: 0 added, 0 removed, 1 changed, 0 unchanged\ns3/bucket/assets: changed (dataset)\n"
	if out.String() != want {
		t.Fatalf("text output:\n%s\nwant:\n%s", out.String(), want)
	}
	if strings.Contains(out.String(), "\x1b[") {
		t.Fatalf("text contains ANSI: %q", out.String())
	}
}

func TestInspectTextEscapesManifestAndRuntimeControlsWhileJSONRemainsExact(t *testing.T) {
	canary := "safe\nFORGED\x1b[31m\t\u0085end"
	result := inspection.Inspection{
		SchemaVersion: 1, Valid: true, BundleIdentity: canary,
		Source:   inspection.SourceProjection{AccountID: canary, Region: canary},
		Target:   inspection.TargetProjection{FlociVersion: canary},
		Services: []inspection.ServiceSummary{{Service: canary}},
		Resources: []inspection.Resource{{
			Identity: inspection.ResourceIdentity{Service: canary, Type: canary, ID: canary},
			Findings: []inspection.Finding{{Code: canary, Severity: canary, Support: canary, Resource: canary, Property: canary}},
		}},
		Findings: []inspection.Finding{{Code: canary, Severity: canary, Support: canary, Resource: canary, Property: canary}},
		Runtime:  inspection.Runtime{State: inspection.RuntimeUnavailable, FailedScripts: []string{canary}, Diagnostic: canary},
		Receipt: &inspection.Receipt{
			Baseline: canary, Current: canary, Categories: []inspection.ChangeCategory{inspection.ChangeCategory(canary)},
			Resources: []inspection.ResourceChange{{Resource: inspection.ResourceIdentity{Service: canary, Type: canary, ID: canary}, Outcome: inspection.Outcome(canary), Categories: []inspection.ChangeCategory{inspection.ChangeCategory(canary)}}},
		},
	}

	var textOut bytes.Buffer
	textCommand := New(Options{Stdout: &textOut, Stderr: &bytes.Buffer{}, App: &fakeService{inspectResult: result}})
	textCommand.SetArgs([]string{"inspect", "--project", writeProject(t)})
	if err := textCommand.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	text := textOut.String()
	if strings.Contains(text, "\nFORGED") || strings.Contains(text, "\x1b") || strings.Contains(text, "\t") || strings.ContainsRune(text, '\u0085') {
		t.Fatalf("text contains unescaped control data: %q", text)
	}
	for _, escaped := range []string{`safe\x0AFORGED\x1B[31m\x09\x85end`, "Categories: safe"} {
		if !strings.Contains(text, escaped) {
			t.Fatalf("text does not contain %q: %q", escaped, text)
		}
	}

	var jsonOut bytes.Buffer
	jsonCommand := New(Options{Stdout: &jsonOut, Stderr: &bytes.Buffer{}, App: &fakeService{inspectResult: result}})
	jsonCommand.SetArgs([]string{"inspect", "--project", writeProject(t), "--output", "json"})
	if err := jsonCommand.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Data inspection.Inspection `json:"data"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data.BundleIdentity != canary || envelope.Data.Runtime.Diagnostic != canary || envelope.Data.Findings[0].Support != canary {
		t.Fatalf("JSON inspection was sanitized: %#v", envelope.Data)
	}
}

func TestTerminalSafeEscapesUnicodeFormattingAndPreservesReadableUnicode(t *testing.T) {
	input := "café 日本語 🙂 \u202Ereversed\u2066isolated\u2069"
	want := `café 日本語 🙂 \u202Ereversed\u2066isolated\u2069`

	if got := terminalSafe(input); got != want {
		t.Fatalf("terminalSafe() = %q, want %q", got, want)
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
	if options.Progress != nil {
		options.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "dynamodb", Resource: "orders", CompletedRecords: 5, TotalRecords: 10, RemainingRecords: 5, TotalPrecision: "estimated"})
	}
	return pullReceiptResult(), nil
}

func pullReceiptResult() app.PullResult {
	return app.PullResult{
		Manifest: model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Source: model.SourceMetadata{AccountID: "123456789012", Region: "eu-west-1"}, Findings: []model.Finding{{Code: "DATA_CAPTURE_PARTIAL", Severity: "warning"}}},
		Baseline: app.BaselinePresent,
		Receipt: &inspection.Receipt{
			SchemaVersion: 1,
			Baseline:      "sha256:baseline",
			Current:       "sha256:current",
			Counts:        inspection.ReceiptCounts{Added: 1, Removed: 2, Changed: 3, Unchanged: 4},
			Resources: []inspection.ResourceChange{{
				Resource:   inspection.ResourceIdentity{Service: "dynamodb", Type: "table", ID: "orders"},
				Outcome:    inspection.OutcomeChanged,
				Categories: []inspection.ChangeCategory{inspection.CategoryDataset, inspection.CategoryGovernance},
				Units:      []inspection.UnitDecision{{ID: "chunk-000001", Outcome: "refreshed", Reason: "freshness_unproven", ArtifactCount: 1, ArtifactBytes: 42}},
			}},
		},
	}
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
	if envelope.Command != "pull" || envelope.Status != StatusSuccessWithFindings {
		t.Fatalf("pull envelope = %#v", envelope)
	}
	payload, ok := envelope.Data.(map[string]any)
	if !ok || payload["baseline"] != string(app.BaselinePresent) || payload["schema_version"] != float64(model.CurrentManifestSchemaVersion) || payload["source"] == nil || payload["findings"] == nil || payload["manifest"] != nil {
		t.Fatalf("pull payload = %#v", envelope.Data)
	}
	receipt, ok := payload["receipt"].(map[string]any)
	if !ok || receipt["baseline"] != "sha256:baseline" || receipt["current"] != "sha256:current" {
		t.Fatalf("pull receipt = %#v", payload["receipt"])
	}
	counts := receipt["counts"].(map[string]any)
	if counts["added"] != float64(1) || counts["removed"] != float64(2) || counts["changed"] != float64(3) || counts["unchanged"] != float64(4) {
		t.Fatalf("pull receipt counts = %#v", counts)
	}
	resources := receipt["resources"].([]any)
	change := resources[0].(map[string]any)
	if !reflect.DeepEqual(change["categories"], []any{"dataset", "governance"}) {
		t.Fatalf("pull receipt categories = %#v", change["categories"])
	}
	units := change["units"].([]any)
	unit := units[0].(map[string]any)
	if unit["id"] != "chunk-000001" || unit["outcome"] != "refreshed" || unit["reason"] != "freshness_unproven" || unit["artifact_count"] != float64(1) || unit["artifact_bytes"] != float64(42) {
		t.Fatalf("pull unit decision = %#v", unit)
	}
	if strings.Contains(stdout.String(), "secret-canary") {
		t.Fatalf("pull output disclosed privacy canary: %s", stdout.String())
	}
	var event model.ProgressEvent
	if err := json.Unmarshal(stderr.Bytes(), &event); err != nil {
		t.Fatalf("stderr progress is not JSON: %q: %v", stderr.String(), err)
	}
	if event.Resource != "orders" || event.TotalPrecision != "estimated" || event.RemainingRecords != 5 {
		t.Fatalf("event = %#v", event)
	}
}

func TestPullTextIncludesExactReceiptWithoutPrivacyCanary(t *testing.T) {
	service := &progressService{}
	var stdout, stderr bytes.Buffer
	cmd := New(Options{Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr, App: service})
	cmd.SetArgs([]string{"pull", "--project", writeProject(t), "--yes", "--progress", "off", "--work-dir", t.TempDir()})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"baseline": "present"`, `"baseline": "sha256:baseline"`, `"current": "sha256:current"`, `"added": 1`, `"removed": 2`, `"changed": 3`, `"unchanged": 4`, `"service": "dynamodb"`, `"id": "orders"`, `"dataset"`, `"governance"`, `"outcome": "refreshed"`, `"reason": "freshness_unproven"`} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("text output missing %s:\n%s", expected, stdout.String())
		}
	}
	if !strings.Contains(stdout.String(), "capture dynamodb/table/orders reused=0 refreshed=1 invalidated=0 reasons=freshness_unproven") {
		t.Fatalf("text output missing concise capture summary:\n%s", stdout.String())
	}
	if strings.Contains(stdout.String(), "secret-canary") || stderr.Len() != 0 {
		t.Fatalf("text output disclosed canary or emitted progress: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestPullRestartHelpOnlyDiscardsMatchingCheckpoint(t *testing.T) {
	var stdout bytes.Buffer
	cmd := New(Options{Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stdout, App: &fakeService{}})
	cmd.SetArgs([]string{"pull", "--help"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(stdout.String())
	if !strings.Contains(text, "discard the matching capture checkpoint") || strings.Contains(text, "ledger cleared") || strings.Contains(text, "clear the ledger") {
		t.Fatalf("restart help misstates retention semantics:\n%s", stdout.String())
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
	if line != "capture dynamodb orders 5/~10 5 remaining" {
		t.Fatalf("plain progress = %q", line)
	}
}

func TestProgressBytesLabelScalesLargeCaptures(t *testing.T) {
	if got := progressBytesLabel(2 << 30); got != "2.0 GiB" {
		t.Fatalf("progressBytesLabel() = %q, want 2.0 GiB", got)
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
func (f *fakeService) UpWithOptions(_ context.Context, _ config.Project, _ string, options app.UpOptions) error {
	f.started = true
	f.wait = options.Wait
	return nil
}

func (f *fakeService) Down(_ context.Context, _ config.Project, dir string) error {
	f.downCalled = true
	f.downDir = dir
	return f.downErr
}
func (f *fakeService) Reset(context.Context, config.Project, string) error {
	f.resetCalled = true
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
