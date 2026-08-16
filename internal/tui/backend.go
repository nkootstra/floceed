package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nkootstra/floceed/internal/app"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"go.yaml.in/yaml/v3"
)

type Profile struct {
	Name   string
	Region string
}

type ProjectRequest struct {
	Project        config.Project
	ProjectFile    string
	FixtureProfile string
	Progress       func(model.ProgressEvent)
}

type Backend interface {
	Profiles(context.Context) ([]Profile, error)
	Identity(context.Context, string, string) (awsconfig.Identity, error)
	Scan(context.Context, app.ScanRequest) (app.ScanResult, error)
	Plan(context.Context, ProjectRequest) (app.Plan, error)
	SaveAndPull(context.Context, ProjectRequest) (model.Manifest, error)
}

// application is satisfied by *app.Application; it exists so SaveAndPull's
// write path is testable without AWS.
type application interface {
	Identity(context.Context, string, string) (awsconfig.Identity, error)
	Scan(context.Context, app.ScanRequest) (app.ScanResult, error)
	PlanWithOptions(context.Context, config.Project, app.PlanOptions) (app.Plan, error)
	PullWithOptions(context.Context, config.Project, string, string, string, app.PullOptions) (app.PullResult, error)
}

// ApplicationBackend adapts *app.Application to the TUI Backend interface.
type ApplicationBackend struct{ App application }

func (b ApplicationBackend) Profiles(context.Context) ([]Profile, error) {
	names, err := awsconfig.AvailableProfiles()
	if err != nil {
		return nil, err
	}
	profiles := make([]Profile, len(names))
	for i, name := range names {
		profiles[i].Name = name
	}
	return profiles, nil
}

func (b ApplicationBackend) Identity(ctx context.Context, profile, region string) (awsconfig.Identity, error) {
	return b.App.Identity(ctx, profile, region)
}

func (b ApplicationBackend) Scan(ctx context.Context, req app.ScanRequest) (app.ScanResult, error) {
	return b.App.Scan(ctx, req)
}

func (b ApplicationBackend) Plan(ctx context.Context, req ProjectRequest) (app.Plan, error) {
	if err := req.Project.Validate(); err != nil {
		return app.Plan{}, err
	}
	return b.App.PlanWithOptions(ctx, req.Project, app.PlanOptions{AWSProfile: req.Project.Source.Profile, Region: req.Project.Source.Region, FixtureProfile: req.FixtureProfile})
}

func (b ApplicationBackend) SaveAndPull(ctx context.Context, req ProjectRequest) (model.Manifest, error) {
	if err := req.Project.Validate(); err != nil {
		return model.Manifest{}, err
	}
	abs, err := filepath.Abs(req.ProjectFile)
	if err != nil {
		return model.Manifest{}, err
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return model.Manifest{}, err
	}
	data, err := yaml.Marshal(req.Project)
	if err != nil {
		return model.Manifest{}, fmt.Errorf("encode floceed project: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".floceed-project-*")
	if err != nil {
		return model.Manifest{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	writeErr := writeAndSync(tmp, data)
	closeErr := tmp.Close()
	if writeErr != nil {
		return model.Manifest{}, writeErr
	}
	if closeErr != nil {
		return model.Manifest{}, closeErr
	}
	if err := os.Rename(tmpName, abs); err != nil {
		return model.Manifest{}, err
	}
	result, err := b.App.PullWithOptions(ctx, req.Project, dir, req.Project.Source.Profile, req.Project.Source.Region, app.PullOptions{Progress: req.Progress, FixtureProfile: req.FixtureProfile})
	if err != nil {
		return model.Manifest{}, err
	}
	return result.Manifest, nil
}

// writeAndSync applies restrictive permissions, writes the payload, and
// fsyncs it. It returns the first failure so a truncated project file is
// never renamed into place.
func writeAndSync(f *os.File, data []byte) error {
	if err := f.Chmod(0600); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}
