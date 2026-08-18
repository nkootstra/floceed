package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/captureledger"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/nkootstra/floceed/internal/model"
	snsservice "github.com/nkootstra/floceed/internal/services/sns"
	sqsservice "github.com/nkootstra/floceed/internal/services/sqs"
)

type discoveryAdapter struct {
	name    string
	started chan<- string
	release <-chan struct{}
	err     error
}

func (a *discoveryAdapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: a.name}
}
func (*discoveryAdapter) Plan(config.Project, bool) catalog.PlanContribution {
	return catalog.PlanContribution{}
}
func (*discoveryAdapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}
func (a *discoveryAdapter) Discover(ctx context.Context, _ model.SourceScope) (model.DiscoveryResult, error) {
	a.started <- a.name
	select {
	case <-a.release:
		return model.DiscoveryResult{}, a.err
	case <-ctx.Done():
		return model.DiscoveryResult{}, ctx.Err()
	}
}
func (*discoveryAdapter) Capture(context.Context, model.SourceScope, model.ResourceRef, model.CaptureOptions) (*model.Snapshot, error) {
	return nil, errors.New("not implemented")
}
func (*discoveryAdapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }
func (*discoveryAdapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding {
	return nil
}

type registryFactory struct{ registry *catalog.Registry }

func (f registryFactory) Open(context.Context, SourceRequest) (Source, error) {
	return Source{Registry: f.registry}, nil
}

func TestErrorUsesMessageAndSupportsUnwrap(t *testing.T) {
	cause := errors.New("root cause")
	err := &Error{Message: "capture failed", Err: cause}
	if got := err.Error(); got != "capture failed" {
		t.Fatalf("Error() = %q, want %q", got, "capture failed")
	}
	if !errors.Is(err, cause) {
		t.Fatal("Error does not unwrap its cause")
	}
}

func TestEmptyErrorHasStableFallback(t *testing.T) {
	err := &Error{}
	if got := err.Error(); got != "floceed operation failed" {
		t.Fatalf("Error() = %q, want stable fallback", got)
	}
}

func TestScanDiscoversServicesConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	registry, err := catalog.New(
		&discoveryAdapter{name: "s3", started: started, release: release},
		&discoveryAdapter{name: "dynamodb", started: started, release: release},
	)
	if err != nil {
		t.Fatal(err)
	}
	service := New("test")
	service.Factory = registryFactory{registry: registry}
	done := make(chan error, 1)
	go func() {
		_, err := service.Scan(context.Background(), ScanRequest{})
		done <- err
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			<-done
			t.Fatal("service discovery ran sequentially")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPlanIncludesExplicitEventDependencySelectionsWithoutIAM(t *testing.T) {
	registry, err := catalog.New(sqsservice.New(), snsservice.New())
	if err != nil {
		t.Fatal(err)
	}
	service := New("test")
	service.Factory = registryFactory{registry: registry}
	plan, err := service.Plan(context.Background(), config.Project{
		SchemaVersion: config.CurrentSchemaVersion,
		Source:        config.Source{Region: "eu-west-1"},
		Resources: config.Resources{
			SQS: []config.SQSResource{{Name: "jobs", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs"}},
			SNS: []config.SNSResource{{Name: "events", ARN: "arn:aws:sns:eu-west-1:123456789012:events"}},
		},
	}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Selected) != 2 || len(plan.RequiredIAMActions) != 1 || plan.RequiredIAMActions[0] != "sts:GetCallerIdentity" {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestScanSkipsDeselectedServices(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	close(release)
	registry, err := catalog.New(
		&discoveryAdapter{name: "s3", started: started, release: release},
		&discoveryAdapter{name: "dynamodb", started: started, release: release, err: errors.New("should not become a finding")},
	)
	if err != nil {
		t.Fatal(err)
	}
	service := New("test")
	service.Factory = registryFactory{registry: registry}

	result, err := service.Scan(context.Background(), ScanRequest{Services: []string{"s3"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Findings) != 0 {
		t.Fatalf("findings = %#v, want none", result.Findings)
	}
	select {
	case got := <-started:
		if got != "s3" {
			t.Fatalf("discovered service = %q, want s3", got)
		}
	default:
		t.Fatal("selected service was not discovered")
	}
	select {
	case got := <-started:
		t.Fatalf("deselected service %q was discovered", got)
	default:
	}
}

type adapter struct {
	captures    int
	lastOptions model.CaptureOptions
}

func (*adapter) Service() model.ServiceDescriptor { return model.ServiceDescriptor{Name: "s3"} }
func (*adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}
func (a *adapter) Capture(_ context.Context, scope model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	a.captures++
	a.lastOptions = opts
	return model.NewSnapshot(ref, "s3", map[string]any{"name": ref.ID, "region": scope.Region})
}
func (*adapter) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (*adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
func (*adapter) Plan(p config.Project, _ bool) catalog.PlanContribution {
	var contribution catalog.PlanContribution
	for _, resource := range p.Resources.S3 {
		contribution.Selections = append(contribution.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: resource.Name, ARN: "arn:aws:s3:::" + resource.Name}})
	}
	contribution.RequiredIAMActions = []string{"s3:GetBucketLocation"}
	return contribution
}
func (*adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}

type factory struct {
	adapter *adapter
	calls   int
	request SourceRequest
}

type adapterFactory struct{ adapter catalog.Adapter }

func (f adapterFactory) Open(_ context.Context, req SourceRequest) (Source, error) {
	r, err := catalog.New(f.adapter)
	if err != nil {
		return Source{}, err
	}
	return Source{Scope: model.SourceScope{Profile: req.Profile, Region: req.Region, AccountID: "123456789012"}, Identity: awsconfig.Identity{AccountID: "123456789012"}, Registry: r}, nil
}

type blockingCaptureAdapter struct {
	adapter
	started chan<- struct{}
	release <-chan struct{}
}

func (a *blockingCaptureAdapter) Capture(ctx context.Context, scope model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	a.started <- struct{}{}
	select {
	case <-a.release:
		return a.adapter.Capture(ctx, scope, ref, opts)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *factory) Open(_ context.Context, req SourceRequest) (Source, error) {
	f.calls++
	f.request = req
	r, _ := catalog.New(f.adapter)
	return Source{Scope: model.SourceScope{Profile: req.Profile, Region: req.Region, AccountID: "123456789012"}, Identity: awsconfig.Identity{AccountID: "123456789012"}, Registry: r}, nil
}

func TestPlanResolvesFixtureProfileBeforeOpeningSource(t *testing.T) {
	f := &factory{adapter: &adapter{}}
	service := New("test")
	service.Factory = f
	p := config.NewProject()
	p.Source.Region = "eu-west-1"
	p.FixtureProfiles = map[string]config.FixtureProfile{"safe": {}}
	p.Resources.S3 = []config.S3Resource{{Name: "assets"}}

	plan, err := service.PlanWithOptions(context.Background(), p, PlanOptions{FixtureProfile: "safe"})
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Fatalf("source opens = %d, want 1", f.calls)
	}
	if plan.Governance == nil || plan.Governance.Profile != "safe" || plan.Governance.PolicyIdentity == "" {
		t.Fatalf("governance = %#v, want resolved safe profile", plan.Governance)
	}
	if f.adapter.lastOptions.Governance == nil || f.adapter.lastOptions.Governance.Identity != plan.Governance.PolicyIdentity {
		t.Fatalf("adapter governance = %#v, want plan identity %q", f.adapter.lastOptions.Governance, plan.Governance.PolicyIdentity)
	}
}

func TestPlanRejectsUnknownFixtureProfileBeforeOpeningSource(t *testing.T) {
	f := &factory{adapter: &adapter{}}
	service := New("test")
	service.Factory = f
	p := config.NewProject()
	p.Source.Region = "eu-west-1"

	_, err := service.PlanWithOptions(context.Background(), p, PlanOptions{FixtureProfile: "missing"})
	if err == nil || !strings.Contains(err.Error(), "unknown fixture profile") {
		t.Fatalf("PlanWithOptions() error = %v", err)
	}
	if f.calls != 0 {
		t.Fatalf("source opened %d times before profile validation", f.calls)
	}
}

func TestPullRejectsUnknownFixtureProfileBeforeSourceOrCheckpointCreation(t *testing.T) {
	f := &factory{adapter: &adapter{}}
	service := New("test")
	service.Factory = f
	p := config.NewProject()
	p.Source.Region = "eu-west-1"
	workDir := filepath.Join(t.TempDir(), "captures")

	_, err := service.PullWithOptions(context.Background(), p, t.TempDir(), "", "", PullOptions{WorkDir: workDir, FixtureProfile: "missing"})
	if err == nil || !strings.Contains(err.Error(), "unknown fixture profile") {
		t.Fatalf("PullWithOptions() error = %v", err)
	}
	if f.calls != 0 {
		t.Fatalf("source opened %d times before fixture validation", f.calls)
	}
	if _, statErr := os.Stat(workDir); !os.IsNotExist(statErr) {
		t.Fatalf("checkpoint root exists before fixture validation: %v", statErr)
	}
}

func TestManifestCarriesDisclosureBoundedGovernanceAudit(t *testing.T) {
	service := New("test")
	planned := Plan{Governance: &model.GovernanceAudit{Profile: "safe", PolicyIdentity: "opaque-policy", Rules: []model.GovernanceRuleAudit{{RuleID: "rule-001", Action: "omit", Count: model.CountBucket1To9}}}}
	manifest := service.manifest(config.Project{}, planned, nil)
	if manifest.Governance == nil || !reflect.DeepEqual(manifest.Governance, planned.Governance) {
		t.Fatalf("manifest governance = %#v, want %#v", manifest.Governance, planned.Governance)
	}
}

func TestCaptureFingerprintChangesWithGovernancePolicy(t *testing.T) {
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.FixtureProfiles = map[string]config.FixtureProfile{
		"one": {Rules: []config.GovernanceRule{{ID: "rule-001", Service: "s3", Resource: "assets", Target: config.GovernanceTarget{Kind: "s3_text_body"}, Action: "replace", Replacement: "one"}}},
		"two": {Rules: []config.GovernanceRule{{ID: "rule-001", Service: "s3", Resource: "assets", Target: config.GovernanceTarget{Kind: "s3_text_body"}, Action: "replace", Replacement: "two"}}},
	}
	one, err := project.ResolveFixtureProfile("one", nil)
	if err != nil {
		t.Fatal(err)
	}
	two, err := project.ResolveFixtureProfile("two", nil)
	if err != nil {
		t.Fatal(err)
	}
	left := captureFingerprint(project, "/project", "aws", "eu-west-1", "123456789012", one)
	right := captureFingerprint(project, "/project", "aws", "eu-west-1", "123456789012", two)
	if left == right {
		t.Fatal("capture fingerprint did not change with governance policy")
	}
}

func TestCaptureFingerprintChangesWhenGovernanceSecretRotates(t *testing.T) {
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	project.FixtureProfiles = map[string]config.FixtureProfile{"safe": {Rules: []config.GovernanceRule{{ID: "rule-001", Service: "s3", Resource: "assets", Target: config.GovernanceTarget{Kind: "s3_text_body"}, Action: "pseudonymize", KeyID: "fixture-key", Scope: "project", Algorithm: "pseudonym/v1", ContentTypes: []string{"text/plain"}}}}}
	secretOne := "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
	secretTwo := "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA="
	one, err := project.ResolveFixtureProfile("safe", func(string) string { return secretOne })
	if err != nil {
		t.Fatal(err)
	}
	two, err := project.ResolveFixtureProfile("safe", func(string) string { return secretTwo })
	if err != nil {
		t.Fatal(err)
	}
	if captureFingerprint(project, "/project", "aws", "eu-west-1", "123456789012", one) == captureFingerprint(project, "/project", "aws", "eu-west-1", "123456789012", two) {
		t.Fatal("capture fingerprint did not change after governance secret rotation")
	}
}

func TestPlanCapturesInMemoryAndReportsIAM(t *testing.T) {
	a := &adapter{}
	f := &factory{adapter: a}
	service := New("test")
	service.Factory = f
	p := config.Project{SchemaVersion: 1, Source: config.Source{Region: "eu-west-1"}, Target: config.Target{FlociVersion: "1.6.0"}, Resources: config.Resources{S3: []config.S3Resource{{Name: "assets"}}}}
	plan, err := service.Plan(context.Background(), p, "dev", "")
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 || a.captures != 1 {
		t.Fatalf("factory=%d captures=%d", f.calls, a.captures)
	}
	if len(plan.Operations) != 2 || len(plan.RequiredIAMActions) == 0 {
		t.Fatalf("incomplete plan: %#v", plan)
	}
}

func TestOperationsMakeResolvedTargetsPrecedeLinks(t *testing.T) {
	snapshot := &model.Snapshot{Service: "s3", Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: "assets"}}
	ops := operations(snapshot, []model.Dependency{{To: model.ResourceRef{Service: "sqs", Type: "queue", ID: "jobs"}}})
	if len(ops) != 3 {
		t.Fatalf("operations = %#v", ops)
	}
	link := ops[2]
	if link.ID != "links:s3:assets" || !slices.Contains(link.DependsOn, "mutable:s3:assets") || !slices.Contains(link.DependsOn, "mutable:sqs:jobs") {
		t.Fatalf("link dependencies = %#v", link)
	}
}

func TestDependencyResolutionRequiresExactARN(t *testing.T) {
	selected := map[string]model.ResourceRef{
		resourceIdentityKey("sqs", "queue", "jobs"): {Service: "sqs", Type: "queue", ID: "jobs", ARN: "arn:aws:sqs:eu-west-1:123456789012:jobs"},
	}
	if !dependencyResolved(model.Dependency{To: selected[resourceIdentityKey("sqs", "queue", "jobs")]}, selected) {
		t.Fatal("exact selected ARN was not resolved")
	}
	for _, dependency := range []model.Dependency{
		{To: model.ResourceRef{Service: "sqs", Type: "queue", ID: "jobs"}},
		{To: model.ResourceRef{Service: "sqs", Type: "queue", ID: "jobs", ARN: "arn:aws:sqs:eu-west-1:999999999999:jobs"}},
	} {
		if dependencyResolved(dependency, selected) {
			t.Fatalf("dependency incorrectly resolved: %#v", dependency.To)
		}
	}
}

func TestPlanRejectsUnexpectedAccount(t *testing.T) {
	a := &adapter{}
	service := New("test")
	service.Factory = &factory{adapter: a}
	p := config.Project{SchemaVersion: 1, Source: config.Source{Region: "eu-west-1", ExpectedAccountID: "999999999999"}, Target: config.Target{FlociVersion: "1.6.0"}}
	_, err := service.Plan(context.Background(), p, "", "")
	if err == nil {
		t.Fatal("expected mismatch")
	}
}

func TestDockerProbeHasBoundedContext(t *testing.T) {
	runtime := newDockerLocalRuntime()
	runtime.probeTimeout = 20 * time.Millisecond
	runtime.dockerCommand = func(ctx context.Context, _ ...string) ([]byte, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("Docker probe context has no deadline")
		}
		if remaining := time.Until(deadline); remaining <= 0 || remaining > runtime.probeTimeout {
			t.Fatalf("Docker probe deadline remaining = %v, want within %v", remaining, runtime.probeTimeout)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	started := time.Now()
	_, err := runtime.runDockerProbe(context.Background(), "info")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runDockerProbe() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("runDockerProbe() took %v, want a bounded probe", elapsed)
	}
}

func TestDecodeReplayProgressFromComposeLog(t *testing.T) {
	event, ok := decodeProgressLine(`floci | FLOCEED_PROGRESS {"schema_version":1,"event":"progress","operation":"replay","phase":"data","service":"s3","resource":"assets","completed_records":9,"total_records":10,"total_precision":"exact"}`)
	if !ok {
		t.Fatal("progress line was not decoded")
	}
	if event.Resource != "assets" || event.CompletedRecords != 9 || event.TotalRecords != 10 {
		t.Fatalf("event = %#v", event)
	}
	if _, ok := decodeProgressLine("ordinary Floci log"); ok {
		t.Fatal("ordinary log decoded as progress")
	}
}

func TestCaptureLockExcludesConcurrentHolderAndReleases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capture.lock")
	release, err := acquireCaptureLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireCaptureLock(path); !errors.Is(err, errCaptureLocked) {
		release()
		t.Fatalf("second lock error = %v, want errCaptureLocked", err)
	}
	release()
	release2, err := acquireCaptureLock(path)
	if err != nil {
		t.Fatalf("lock after release = %v", err)
	}
	release2()
}

func TestDockerProbePreservesCallerCancellation(t *testing.T) {
	runtime := newDockerLocalRuntime()
	runtime.probeTimeout = time.Minute
	runtime.dockerCommand = func(ctx context.Context, _ ...string) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := runtime.runDockerProbe(ctx, "info")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runDockerProbe() error = %v, want canceled", err)
	}
}

func testProject() config.Project {
	p := config.NewProject()
	p.Source.Region = "eu-west-1"
	p.Resources.S3 = []config.S3Resource{{Name: "assets"}}
	return p
}

func requireAppError(t *testing.T, err error, kind ErrorKind, code string) *Error {
	t.Helper()
	var appErr *Error
	if !errors.As(err, &appErr) {
		t.Fatalf("error = %v, want *app.Error", err)
	}
	if appErr.Kind != kind || appErr.Code != code {
		t.Fatalf("error = {Kind:%q Code:%q}, want {Kind:%q Code:%q}", appErr.Kind, appErr.Code, kind, code)
	}
	return appErr
}

func projectWithComposeFile(t *testing.T, p config.Project) string {
	t.Helper()
	projectDir := t.TempDir()
	target := filepath.Join(projectDir, p.Output.Directory)
	if err := os.MkdirAll(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, bundle.ComposeFile), []byte("services: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return projectDir
}

func TestPullWritesDeterministicBundleWithoutDataArtifacts(t *testing.T) {
	projectDir := t.TempDir()
	p := testProject()
	captureAdapter := &adapter{}
	service := New("v1.2.3")
	service.Factory = &factory{adapter: captureAdapter}
	capturedAt := time.Date(2026, time.August, 14, 13, 45, 0, 0, time.FixedZone("test", 2*60*60))
	service.Now = func() time.Time { return capturedAt }
	var validatedCompose string
	service.ComposeValidator = func(_ context.Context, filename string) error {
		validatedCompose = filename
		if _, err := os.Stat(filename); err != nil {
			return err
		}
		return nil
	}

	result, err := service.Pull(context.Background(), p, projectDir, "dev", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Baseline != BaselineAbsent || result.Receipt != nil {
		t.Fatalf("first pull baseline = %q, receipt = %#v", result.Baseline, result.Receipt)
	}
	manifest := result.Manifest
	if captureAdapter.captures != 1 {
		t.Fatalf("captures = %d, want 1", captureAdapter.captures)
	}
	if manifest.Tool.Version != "v1.2.3" || !manifest.Capture.CapturedAt.Equal(capturedAt.UTC()) {
		t.Fatalf("manifest metadata = %#v", manifest)
	}
	if manifest.Source.AccountID != "123456789012" || manifest.Source.Region != p.Source.Region {
		t.Fatalf("manifest source = %#v", manifest.Source)
	}
	if len(manifest.Snapshots) != 1 || len(manifest.Snapshots[0].Data) != 0 {
		t.Fatalf("manifest snapshots = %#v", manifest.Snapshots)
	}
	target := filepath.Join(projectDir, p.Output.Directory)
	if filepath.Base(validatedCompose) != bundle.ComposeFile || filepath.Dir(filepath.Dir(validatedCompose)) != projectDir {
		t.Fatalf("validated compose = %q", validatedCompose)
	}
	manifestJSON, err := os.ReadFile(filepath.Join(target, "bundle", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var written model.Manifest
	if err := json.Unmarshal(manifestJSON, &written); err != nil {
		t.Fatal(err)
	}
	if !equalJSON(t, written, manifest) {
		t.Fatalf("written manifest differs:\n got: %#v\nwant: %#v", written, manifest)
	}
	if matches, err := filepath.Glob(filepath.Join(target, "bundle", "data", "*")); err != nil || len(matches) != 0 {
		t.Fatalf("data artifacts = %v, error = %v", matches, err)
	}
}

func TestPullReturnsReceiptWhenReplacingEquivalentBundle(t *testing.T) {
	projectDir := t.TempDir()
	p := testProject()
	service := New("test")
	service.Factory = &factory{adapter: &adapter{}}
	service.ComposeValidator = func(context.Context, string) error { return nil }

	first, err := service.Pull(context.Background(), p, projectDir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Pull(context.Background(), p, projectDir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Baseline != BaselineAbsent || second.Baseline != BaselinePresent || second.Receipt == nil {
		t.Fatalf("pull results = first %#v, second %#v", first, second)
	}
	if second.Receipt.Counts.Changed != 0 || second.Receipt.Counts.Added != 0 || second.Receipt.Counts.Removed != 0 {
		t.Fatalf("equivalent receipt = %#v", second.Receipt)
	}
}

func TestPullRejectsInvalidBaselineBeforeOpeningSource(t *testing.T) {
	projectDir := t.TempDir()
	p := testProject()
	target := filepath.Join(projectDir, p.Output.Directory)
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "corrupt"), []byte("secret-canary"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &factory{adapter: &adapter{}}
	service := New("test")
	service.Factory = f

	_, err := service.Pull(context.Background(), p, projectDir, "", "")
	requireAppError(t, err, ErrorFilesystem, "BUNDLE_INTEGRITY_INVALID")
	if f.calls != 0 {
		t.Fatalf("source opened %d times for invalid baseline", f.calls)
	}
	contents, readErr := os.ReadFile(filepath.Join(target, "corrupt"))
	if readErr != nil || string(contents) != "secret-canary" {
		t.Fatalf("invalid baseline changed: %q, %v", contents, readErr)
	}
}

func TestPullReturnsChangedReceiptAfterSuccessfulReplacement(t *testing.T) {
	projectDir := t.TempDir()
	p := testProject()
	service := New("test")
	service.Factory = &factory{adapter: &adapter{}}
	service.ComposeValidator = func(context.Context, string) error { return nil }
	if _, err := service.Pull(context.Background(), p, projectDir, "", ""); err != nil {
		t.Fatal(err)
	}

	result, err := service.Pull(context.Background(), p, projectDir, "", "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if result.Baseline != BaselinePresent || result.Receipt == nil || result.Receipt.Counts.Changed != 1 {
		t.Fatalf("changed pull result = %#v", result)
	}
	serialized, err := json.Marshal(result.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-canary", `"structure":{`, "replacement-value", "governance-salt"} {
		if bytes.Contains(serialized, []byte(forbidden)) {
			t.Fatalf("pull receipt disclosed %q: %s", forbidden, serialized)
		}
	}
}

func TestLedgerDecisionsPreserveReceiptClassificationsAndSortUnits(t *testing.T) {
	receipt := inspection.Receipt{
		Counts: inspection.ReceiptCounts{Removed: 1, Unchanged: 1},
		Resources: []inspection.ResourceChange{
			{Resource: inspection.ResourceIdentity{Service: "s3", Type: "bucket", ID: "assets"}, Outcome: inspection.OutcomeUnchanged},
			{Resource: inspection.ResourceIdentity{Service: "s3", Type: "bucket", ID: "old"}, Outcome: inspection.OutcomeRemoved},
		},
	}
	generation := captureledger.Generation{ID: strings.Repeat("a", 64), Resources: []captureledger.Resource{{
		Descriptor: captureledger.ResourceDescriptor{Service: "s3", Type: "bucket", ID: "assets"},
		Units: []captureledger.Unit{
			{ID: "pack-2", Outcome: captureledger.UnitOutcomeRefreshed, Reason: captureledger.ReasonSourceContentChanged},
			{ID: "pack-1", Outcome: captureledger.UnitOutcomeRefreshed, Reason: captureledger.ReasonCaptureDefinitionChanged},
		},
	}}}
	attachLedgerDecisions(&receipt, generation, map[string]string{"s3\x00bucket\x00assets": strings.Repeat("b", 64)})

	if receipt.Counts != (inspection.ReceiptCounts{Removed: 1, Unchanged: 1}) {
		t.Fatalf("receipt counts = %#v", receipt.Counts)
	}
	unchanged, removed := receipt.Resources[0], receipt.Resources[1]
	if unchanged.Outcome != inspection.OutcomeUnchanged || len(unchanged.Categories) != 0 {
		t.Fatalf("capture decision changed semantic classification = %#v", unchanged)
	}
	if len(unchanged.Units) != 2 || unchanged.Units[0].ID != "pack-1" || unchanged.Units[1].ID != "pack-2" {
		t.Fatalf("unit order = %#v", unchanged.Units)
	}
	if removed.Outcome != inspection.OutcomeRemoved || len(removed.Units) != 0 {
		t.Fatalf("selection removal classification changed = %#v", removed)
	}
}

func TestPullReceiptUsesBundleActuallyReplacedByConcurrentWriter(t *testing.T) {
	projectDir := t.TempDir()
	p := testProject()
	newService := func(adapter catalog.Adapter) *Application {
		service := New("test")
		service.Factory = adapterFactory{adapter: adapter}
		service.ComposeValidator = func(context.Context, string) error { return nil }
		return service
	}

	if _, err := newService(&adapter{}).Pull(context.Background(), p, projectDir, "", "eu-west-1"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	slow := newService(&blockingCaptureAdapter{started: started, release: release})
	type pullOutcome struct {
		result PullResult
		err    error
	}
	done := make(chan pullOutcome, 1)
	go func() {
		result, err := slow.PullWithOptions(context.Background(), p, projectDir, "", "us-east-1", PullOptions{WorkDir: filepath.Join(projectDir, "slow-work")})
		done <- pullOutcome{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("slow pull did not reach capture")
	}

	concurrent, err := newService(&adapter{}).PullWithOptions(context.Background(), p, projectDir, "", "ap-southeast-2", PullOptions{WorkDir: filepath.Join(projectDir, "writer-work")})
	if err != nil {
		t.Fatal(err)
	}
	concurrentProjection, err := inspection.ProjectManifest(concurrent.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	outcome := <-done
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	if outcome.result.Baseline != BaselinePresent || outcome.result.Receipt == nil {
		t.Fatalf("slow pull result = %#v", outcome.result)
	}
	if outcome.result.Receipt.Baseline != concurrentProjection.Digest {
		t.Fatalf("receipt baseline = %q, want concurrent bundle %q", outcome.result.Receipt.Baseline, concurrentProjection.Digest)
	}
	if outcome.result.Receipt.Baseline == outcome.result.Receipt.Current {
		t.Fatalf("receipt did not distinguish replaced and installed bundles: %#v", outcome.result.Receipt)
	}
}

func TestPullRenderFailurePreservesInstalledBundleAndReturnsNoResult(t *testing.T) {
	projectDir := t.TempDir()
	p := testProject()
	service := New("test")
	service.Factory = &factory{adapter: &adapter{}}
	service.ComposeValidator = func(context.Context, string) error { return nil }
	first, err := service.Pull(context.Background(), p, projectDir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(projectDir, p.Output.Directory)
	before, err := bundle.LoadGenerated(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	service.ComposeValidator = func(context.Context, string) error { return errors.New("render rejected") }

	failed, err := service.Pull(context.Background(), p, projectDir, "", "us-east-1")
	if err == nil {
		t.Fatal("Pull() succeeded despite render failure")
	}
	if !reflect.DeepEqual(failed, PullResult{}) {
		t.Fatalf("failed pull returned success data: %#v", failed)
	}
	after, loadErr := bundle.LoadGenerated(context.Background(), root)
	if loadErr != nil {
		t.Fatalf("prior bundle unreadable after failure: %v", loadErr)
	}
	if !equalJSON(t, before.Manifest, after.Manifest) || !equalJSON(t, first.Manifest, after.Manifest) {
		t.Fatal("failed replacement changed installed manifest")
	}
}

func TestPullCanceledBeforeCaptureReturnsNoResult(t *testing.T) {
	p := testProject()
	f := &factory{adapter: &adapter{}}
	service := New("test")
	service.Factory = f
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := service.Pull(ctx, p, t.TempDir(), "", "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Pull() error = %v, want context canceled", err)
	}
	if !reflect.DeepEqual(result, PullResult{}) {
		t.Fatalf("canceled pull returned success data: %#v", result)
	}
	if f.calls != 0 {
		t.Fatalf("source opened %d times after cancellation", f.calls)
	}
}

func TestRenderClassifiesManifestReadAndDecodeErrors(t *testing.T) {
	p := testProject()
	service := New("test")
	service.ComposeValidator = func(context.Context, string) error { return nil }

	t.Run("missing", func(t *testing.T) {
		_, err := service.Render(context.Background(), p, t.TempDir())
		requireAppError(t, err, ErrorFilesystem, "BUNDLE_FAILED")
	})

	t.Run("malformed", func(t *testing.T) {
		projectDir := t.TempDir()
		manifestPath := filepath.Join(projectDir, p.Output.Directory, "bundle", "manifest.json")
		if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := service.Render(context.Background(), p, projectDir)
		requireAppError(t, err, ErrorFilesystem, "BUNDLE_FAILED")
	})
}

func TestRenderRebuildsExistingLocalBundle(t *testing.T) {
	projectDir := t.TempDir()
	p := testProject()
	service := New("test")
	service.Factory = &factory{adapter: &adapter{}}
	validated := 0
	service.ComposeValidator = func(context.Context, string) error {
		validated++
		return nil
	}
	pull, err := service.Pull(context.Background(), p, projectDir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	want := pull.Manifest
	got, err := service.Render(context.Background(), p, projectDir)
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSON(t, got, want) {
		t.Fatalf("Render() manifest differs:\n got: %#v\nwant: %#v", got, want)
	}
	if validated != 2 {
		t.Fatalf("compose validations = %d, want 2", validated)
	}
}

func equalJSON(t *testing.T, left, right any) bool {
	t.Helper()
	leftJSON, err := json.Marshal(left)
	if err != nil {
		t.Fatal(err)
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(leftJSON, rightJSON)
}

type sourceFactoryFunc func(context.Context, SourceRequest) (Source, error)

func (f sourceFactoryFunc) Open(ctx context.Context, req SourceRequest) (Source, error) {
	return f(ctx, req)
}

type fakeLocalRuntime struct {
	checks       []Check
	start        func(context.Context, string, string) ([]byte, error)
	waitReady    func(context.Context, string, time.Duration) error
	doctorCalled bool
}

func (f *fakeLocalRuntime) DoctorChecks(context.Context) []Check {
	f.doctorCalled = true
	return f.checks
}

func (f *fakeLocalRuntime) Start(ctx context.Context, target, composeFile string) ([]byte, error) {
	return f.start(ctx, target, composeFile)
}

func (f *fakeLocalRuntime) Stop(context.Context, string, string) ([]byte, error) { return nil, nil }

func (f *fakeLocalRuntime) WaitReady(ctx context.Context, url string, wait time.Duration) error {
	return f.waitReady(ctx, url, wait)
}

func (f *fakeLocalRuntime) InspectStatus(context.Context, string, time.Duration) (inspection.Runtime, error) {
	return inspection.Runtime{State: inspection.RuntimeNotRequested}, nil
}

func (f *fakeLocalRuntime) Logs(context.Context, string, string, int) ([]byte, error) {
	return nil, nil
}

func TestDoctorOrchestratesAllChecksWithoutExternalCommands(t *testing.T) {
	p := testProject()
	service := New("test")
	service.Factory = sourceFactoryFunc(func(context.Context, SourceRequest) (Source, error) {
		return Source{}, nil
	})
	runtime := newDockerLocalRuntime()
	runtime.lookPath = func(name string) (string, error) {
		if name != "docker" {
			t.Fatalf("LookPath(%q), want docker", name)
		}
		return "/fake/docker", nil
	}
	var commands []string
	runtime.dockerCommand = func(_ context.Context, args ...string) ([]byte, error) {
		commands = append(commands, strings.Join(args, " "))
		return []byte("ok"), nil
	}
	service.localRuntime = runtime

	result, err := service.Doctor(context.Background(), p, t.TempDir(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	wantChecks := []Check{
		{Name: "project", OK: true, Message: "configuration is valid"},
		{Name: "aws", OK: true, Message: "caller identity confirmed"},
		{Name: "docker", OK: true, Message: "Docker and Compose are available"},
		{Name: "floci-image", OK: true, Message: "pinned Floci 1.6.0 compat image is available"},
		{Name: "output", OK: true, Message: "output directory is writable"},
	}
	if !reflect.DeepEqual(result.Checks, wantChecks) {
		t.Fatalf("checks = %#v, want %#v", result.Checks, wantChecks)
	}
	wantCommands := []string{"compose version", "info", "manifest inspect " + compose.Image}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("docker commands = %v, want %v", commands, wantCommands)
	}
}

func TestCappedBufferBoundsComposeLogs(t *testing.T) {
	buffer := &cappedBuffer{limit: 4}
	if _, err := buffer.Write([]byte("123456")); err != nil {
		t.Fatal(err)
	}
	if got := buffer.String(); got != "1234" {
		t.Fatalf("capped logs = %q", got)
	}
}

func TestDoctorReturnsTypedFailureAndRetainsCheckResults(t *testing.T) {
	p := testProject()
	service := New("test")
	service.Factory = sourceFactoryFunc(func(context.Context, SourceRequest) (Source, error) {
		return Source{}, errors.New("identity unavailable")
	})
	service.localRuntime = &fakeLocalRuntime{checks: []Check{{Name: "docker", OK: false, Message: "docker executable not found"}}}

	result, err := service.Doctor(context.Background(), p, t.TempDir(), "", "")
	requireAppError(t, err, ErrorLocal, "DOCTOR_FAILED")
	if len(result.Checks) != 4 {
		t.Fatalf("checks = %#v, want project, aws, docker, and output", result.Checks)
	}
	if result.Checks[1].Name != "aws" || result.Checks[1].OK || result.Checks[2].Name != "docker" || result.Checks[2].OK {
		t.Fatalf("failure checks = %#v", result.Checks)
	}
}

type httpDoerFunc func(*http.Request) (*http.Response, error)

func (f httpDoerFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

func readyResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestUpClassifiesComposeFailure(t *testing.T) {
	p := testProject()
	projectDir := projectWithComposeFile(t, p)
	service := New("test")
	service.localRuntime = &fakeLocalRuntime{start: func(_ context.Context, target, composeFile string) ([]byte, error) {
		if filepath.Dir(composeFile) != target {
			t.Fatalf("compose file %q is not below target %q", composeFile, target)
		}
		return []byte("daemon unavailable"), errors.New("exit status 1")
	}}
	err := service.Up(context.Background(), p, projectDir, time.Second)
	appErr := requireAppError(t, err, ErrorLocal, "COMPOSE_UP_FAILED")
	if !strings.Contains(appErr.Message, "daemon unavailable") {
		t.Fatalf("error message = %q", appErr.Message)
	}
}

func TestUpRejectsMissingGeneratedComposeFileBeforeDocker(t *testing.T) {
	p := testProject()
	service := New("test")
	service.localRuntime = &fakeLocalRuntime{start: func(context.Context, string, string) ([]byte, error) {
		t.Fatal("Start() called without a generated Compose file")
		return nil, nil
	}}

	err := service.Up(context.Background(), p, t.TempDir(), time.Second)
	appErr := requireAppError(t, err, ErrorFilesystem, "BUNDLE_MISSING")
	if !strings.Contains(appErr.Message, bundle.ComposeFile) ||
		!strings.Contains(appErr.Remediation, "floceed render") ||
		!strings.Contains(appErr.Remediation, "floceed pull") {
		t.Fatalf("error = %#v, want missing path and remediation", appErr)
	}
}

func TestUpPreservesCancellationBeforeCheckingBundle(t *testing.T) {
	p := testProject()
	service := New("test")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := service.Up(ctx, p, t.TempDir(), time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Up() error = %v, want context canceled", err)
	}
}

func TestUpRejectsNonRegularGeneratedComposeFileBeforeDocker(t *testing.T) {
	p := testProject()
	projectDir := t.TempDir()
	composePath := filepath.Join(projectDir, p.Output.Directory, bundle.ComposeFile)
	if err := os.MkdirAll(composePath, 0700); err != nil {
		t.Fatal(err)
	}
	service := New("test")
	service.localRuntime = &fakeLocalRuntime{start: func(context.Context, string, string) ([]byte, error) {
		t.Fatal("Start() called with a non-regular Compose path")
		return nil, nil
	}}

	err := service.Up(context.Background(), p, projectDir, time.Second)
	appErr := requireAppError(t, err, ErrorFilesystem, "BUNDLE_INVALID")
	if !strings.Contains(appErr.Message, "not a regular file") {
		t.Fatalf("error message = %q", appErr.Message)
	}
}

func TestUpClassifiesStatFailure(t *testing.T) {
	p := testProject()
	projectDir := t.TempDir()
	// Make the output directory path a regular file so Lstat fails with
	// ENOTDIR, exercising the generic filesystem error branch.
	target := filepath.Join(projectDir, p.Output.Directory)
	if err := os.WriteFile(target, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	service := New("test")
	service.localRuntime = &fakeLocalRuntime{start: func(context.Context, string, string) ([]byte, error) {
		t.Fatal("Start() called without a stattable Compose path")
		return nil, nil
	}}

	err := service.Up(context.Background(), p, projectDir, time.Second)
	requireAppError(t, err, ErrorFilesystem, "BUNDLE_FAILED")
}

func TestUpBoundsComposeStartupByWaitTimeout(t *testing.T) {
	p := testProject()
	projectDir := projectWithComposeFile(t, p)
	service := New("test")
	startCanceled := make(chan struct{})
	service.localRuntime = &fakeLocalRuntime{
		start: func(ctx context.Context, _, _ string) ([]byte, error) {
			<-ctx.Done()
			close(startCanceled)
			return nil, ctx.Err()
		},
		waitReady: func(context.Context, string, time.Duration) error {
			t.Fatal("WaitReady() called after compose startup timed out")
			return nil
		},
	}

	err := service.Up(context.Background(), p, projectDir, 10*time.Millisecond)
	requireAppError(t, err, ErrorLocal, "FLOCI_READY_TIMEOUT")
	select {
	case <-startCanceled:
	default:
		t.Fatal("Start() context was not canceled by the wait timeout")
	}
}

func TestUpPreservesContextCancellationWhileWaiting(t *testing.T) {
	p := testProject()
	projectDir := projectWithComposeFile(t, p)
	service := New("test")
	service.localRuntime = &fakeLocalRuntime{
		start: func(context.Context, string, string) ([]byte, error) { return nil, nil },
		waitReady: func(ctx context.Context, _ string, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.Up(ctx, p, projectDir, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Up() error = %v, want context canceled", err)
	}
}

func TestUpReturnsReadyWithoutCallingRealDockerOrHTTP(t *testing.T) {
	p := testProject()
	projectDir := projectWithComposeFile(t, p)
	service := New("test")
	service.localRuntime = &fakeLocalRuntime{
		start: func(context.Context, string, string) ([]byte, error) { return nil, nil },
		waitReady: func(_ context.Context, url string, _ time.Duration) error {
			if url != "http://127.0.0.1:4566/_floci/init" {
				t.Fatalf("readiness URL = %q", url)
			}
			return nil
		},
	}
	if err := service.Up(context.Background(), p, projectDir, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestUpClassifiesFailedReadyHook(t *testing.T) {
	p := testProject()
	projectDir := projectWithComposeFile(t, p)
	service := New("test")
	runtime := newDockerLocalRuntime()
	runtime.pollInterval = time.Nanosecond
	runtime.httpClient = httpDoerFunc(func(*http.Request) (*http.Response, error) {
		return readyResponse(`{"completed":{"ready":false},"scripts":{"ready":[{"script":"seed.py","state":"FAILED","return_code":1}]}}`), nil
	})
	service.localRuntime = &fakeLocalRuntime{
		start:     func(context.Context, string, string) ([]byte, error) { return nil, nil },
		waitReady: runtime.WaitReady,
	}
	err := service.Up(context.Background(), p, projectDir, time.Second)
	appErr := requireAppError(t, err, ErrorLocal, "FLOCI_INIT_FAILED")
	if !strings.Contains(appErr.Message, "seed.py") {
		t.Fatalf("error message = %q", appErr.Message)
	}
}

func TestUpTimesOutWithoutReadiness(t *testing.T) {
	p := testProject()
	projectDir := projectWithComposeFile(t, p)
	service := New("test")
	service.localRuntime = &fakeLocalRuntime{
		start: func(context.Context, string, string) ([]byte, error) { return nil, nil },
		waitReady: func(context.Context, string, time.Duration) error {
			return &Error{Kind: ErrorLocal, Code: "FLOCI_READY_TIMEOUT", Message: "Floci initialization did not complete before the timeout"}
		},
	}
	err := service.Up(context.Background(), p, projectDir, time.Nanosecond)
	requireAppError(t, err, ErrorLocal, "FLOCI_READY_TIMEOUT")
}
