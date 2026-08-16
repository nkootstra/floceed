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
	"strings"
	"testing"
	"time"

	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
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

type adapter struct{ captures int }

func (*adapter) Service() model.ServiceDescriptor { return model.ServiceDescriptor{Name: "s3"} }
func (*adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}
func (a *adapter) Capture(_ context.Context, scope model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	a.captures++
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
}

func (f *factory) Open(_ context.Context, req SourceRequest) (Source, error) {
	f.calls++
	r, _ := catalog.New(f.adapter)
	return Source{Scope: model.SourceScope{Profile: req.Profile, Region: req.Region, AccountID: "123456789012"}, Identity: awsconfig.Identity{AccountID: "123456789012"}, Registry: r}, nil
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

	manifest, err := service.Pull(context.Background(), p, projectDir, "dev", "")
	if err != nil {
		t.Fatal(err)
	}
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
	want, err := service.Pull(context.Background(), p, projectDir, "", "")
	if err != nil {
		t.Fatal(err)
	}
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

func (f *fakeLocalRuntime) WaitReady(ctx context.Context, url string, wait time.Duration) error {
	return f.waitReady(ctx, url, wait)
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
