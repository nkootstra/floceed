package tui

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/nkootstra/floceed/internal/app"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"go.yaml.in/yaml/v3"
)

type fakePuller struct {
	called         bool
	fixtureProfile string
}

func (f *fakePuller) PlanWithOptions(_ context.Context, _ config.Project, options app.PlanOptions) (app.Plan, error) {
	f.fixtureProfile = options.FixtureProfile
	return app.Plan{}, nil
}

func (f *fakePuller) PullWithOptions(_ context.Context, _ config.Project, _ string, _ string, _ string, options app.PullOptions) (model.Manifest, error) {
	f.called = true
	f.fixtureProfile = options.FixtureProfile
	return model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion}, nil
}

func TestApplicationBackendForwardsFixtureProfileToPlanAndPull(t *testing.T) {
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	puller := &fakePuller{}
	backend := ApplicationBackend{App: puller}
	req := ProjectRequest{Project: project, ProjectFile: filepath.Join(t.TempDir(), "floceed.yaml"), FixtureProfile: "share-safe"}
	if _, err := backend.Plan(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if puller.fixtureProfile != "share-safe" {
		t.Fatalf("plan fixture profile = %q", puller.fixtureProfile)
	}
	if _, err := backend.SaveAndPull(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if puller.fixtureProfile != "share-safe" {
		t.Fatalf("pull fixture profile = %q", puller.fixtureProfile)
	}
}

func (*fakePuller) Identity(context.Context, string, string) (awsconfig.Identity, error) {
	return awsconfig.Identity{}, nil
}
func (*fakePuller) Scan(context.Context, app.ScanRequest) (app.ScanResult, error) {
	return app.ScanResult{}, nil
}
func (*fakePuller) Plan(context.Context, config.Project, string, string) (app.Plan, error) {
	return app.Plan{}, nil
}
func (f *fakePuller) Pull(context.Context, config.Project, string, string, string) (model.Manifest, error) {
	f.called = true
	return model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion}, nil
}

// TestSaveAndPullReturnsWriteFailureWithoutPublishingOrPulling pins down the
// contract that a failure writing the project file aborts SaveAndPull: the
// error is returned, Pull is not invoked, and no project file is published.
// The file-size limit makes the payload write fail with EFBIG while the
// preceding CreateTemp and Chmod still succeed, isolating the write step.
func TestSaveAndPullReturnsWriteFailureWithoutPublishingOrPulling(t *testing.T) {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Skipf("RLIMIT_FSIZE unavailable: %v", err)
	}
	restore := limit
	limit.Cur = 4
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Skipf("cannot lower RLIMIT_FSIZE: %v", err)
	}
	defer func() {
		syscall.Setrlimit(syscall.RLIMIT_FSIZE, &restore)
		signal.Reset(syscall.SIGXFSZ)
	}()
	// Exceeding RLIMIT_FSIZE raises SIGXFSZ, which would terminate the test
	// process unless ignored; the write itself still returns EFBIG.
	signal.Ignore(syscall.SIGXFSZ)

	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	projectFile := filepath.Join(t.TempDir(), "floceed.yaml")
	puller := &fakePuller{}
	backend := ApplicationBackend{App: puller}

	_, err := backend.SaveAndPull(context.Background(), ProjectRequest{Project: project, ProjectFile: projectFile})
	if err == nil {
		t.Fatal("SaveAndPull() returned nil error after the project-file write failed")
	}
	if puller.called {
		t.Fatal("Pull() was called after the project-file write failed")
	}
	if data, readErr := os.ReadFile(projectFile); readErr == nil {
		t.Fatalf("project file was published despite the write failure: %q", data)
	}
}

// TestSaveAndPullWritesProjectFileAndCallsPull guards the happy path: a
// valid project file is written atomically, no temp files remain, and Pull
// receives the project directory.
func TestSaveAndPullWritesProjectFileAndCallsPull(t *testing.T) {
	project := config.NewProject()
	project.Source.Region = "eu-west-1"
	projectFile := filepath.Join(t.TempDir(), "floceed.yaml")
	puller := &fakePuller{}
	backend := ApplicationBackend{App: puller}

	manifest, err := backend.SaveAndPull(context.Background(), ProjectRequest{Project: project, ProjectFile: projectFile})
	if err != nil {
		t.Fatal(err)
	}
	if !puller.called {
		t.Fatal("Pull() was not called")
	}
	if manifest.SchemaVersion != model.CurrentManifestSchemaVersion {
		t.Fatalf("manifest = %#v", manifest)
	}
	data, err := os.ReadFile(projectFile)
	if err != nil {
		t.Fatal(err)
	}
	var decoded config.Project
	if err := yaml.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("project file is not valid YAML: %v\n%s", err, data)
	}
	if decoded.Source.Region != project.Source.Region {
		t.Fatalf("decoded region = %q, want %q", decoded.Source.Region, project.Source.Region)
	}
	leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(projectFile), ".floceed-project-*"))
	if len(leftovers) != 0 {
		t.Fatalf("leftover temp files: %v", leftovers)
	}
}
