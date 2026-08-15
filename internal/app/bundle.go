package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/compose"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func (a *Application) Pull(ctx context.Context, p config.Project, projectDir, profile, region string) (model.Manifest, error) {
	tmp, err := os.MkdirTemp(projectDir, ".floceed-capture-")
	if err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	defer os.RemoveAll(tmp)
	planned, snapshots, err := a.capture(ctx, captureRequest{
		Project:      p,
		Profile:      profile,
		Region:       region,
		ArtifactRoot: tmp,
		IncludeData:  true,
	})
	if err != nil {
		return model.Manifest{}, err
	}
	manifest := a.manifest(p, planned, snapshots)
	target := filepath.Join(projectDir, filepath.FromSlash(p.Output.Directory))
	if err := bundle.Render(ctx, target, p, manifest, bundle.RenderOptions{ArtifactRoot: tmp, ValidateCompose: a.ComposeValidator}); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	return manifest, nil
}

func (a *Application) Render(ctx context.Context, p config.Project, projectDir string) (model.Manifest, error) {
	target := filepath.Join(projectDir, filepath.FromSlash(p.Output.Directory))
	b, err := os.ReadFile(filepath.Join(target, "bundle", "manifest.json"))
	if err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	var m model.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	if err := m.Validate(); err != nil {
		return model.Manifest{}, &Error{Kind: ErrorPlan, Code: "MANIFEST_INVALID", Message: err.Error(), Err: err}
	}
	if err := bundle.ValidateGenerated(target); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	if err := bundle.Render(ctx, target, p, m, bundle.RenderOptions{ArtifactRoot: target, ValidateCompose: a.ComposeValidator}); err != nil {
		return model.Manifest{}, filesystemError(err)
	}
	return m, nil
}

func (a *Application) manifest(p config.Project, planned Plan, snapshots []model.Snapshot) model.Manifest {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	version := a.Version
	if version == "" {
		version = "dev"
	}
	partial := false
	for _, finding := range planned.Findings {
		partial = partial || finding.Code == "DATA_CAPTURE_PARTIAL"
	}
	return model.Manifest{SchemaVersion: model.CurrentManifestSchemaVersion, Tool: model.ToolMetadata{Version: version}, Target: model.TargetMetadata{FlociVersion: p.Target.FlociVersion, Image: compose.Image}, Source: planned.Source, Capture: model.CaptureMetadata{CapturedAt: now().UTC(), Partial: partial}, Selected: planned.Selected, Snapshots: snapshots, Operations: planned.Operations, Findings: planned.Findings}
}
