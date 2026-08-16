package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/config"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/nkootstra/floceed/internal/model"
)

// Inspect validates and summarizes the installed artifact without consulting
// AWS, Docker, or any runtime endpoint.
func (a *Application) Inspect(ctx context.Context, project config.Project, projectDir string) (inspection.Inspection, error) {
	return a.InspectWithOptions(ctx, project, projectDir, InspectOptions{})
}

// InspectOptions controls optional, offline inspection enrichments.
type InspectOptions struct {
	ComparePath string
	Runtime     bool
}

// InspectWithOptions validates both comparison sides independently and only
// returns an inspection after every requested artifact has proved valid.
func (a *Application) InspectWithOptions(ctx context.Context, project config.Project, projectDir string, options InspectOptions) (inspection.Inspection, error) {
	if err := ctx.Err(); err != nil {
		return inspection.Inspection{}, inspectError(err)
	}
	if err := project.Validate(); err != nil {
		return inspection.Inspection{}, &Error{Kind: ErrorPlan, Code: "PROJECT_INVALID", Message: err.Error(), Remediation: "Fix the project configuration and retry inspection.", Err: err}
	}
	root := filepath.Join(projectDir, filepath.FromSlash(project.Output.Directory))
	result, projection, err := inspectGenerated(ctx, root)
	if err != nil {
		return inspection.Inspection{}, inspectError(err)
	}
	if options.ComparePath != "" {
		baseline, err := loadComparisonProjection(ctx, options.ComparePath)
		if err != nil {
			return inspection.Inspection{}, err
		}
		receipt := inspection.Compare(baseline, projection)
		result.Receipt = &receipt
	}
	if options.Runtime {
		url := fmt.Sprintf("http://127.0.0.1:%d/_floci/init", project.Target.Port)
		result.Runtime, err = a.localRuntime.InspectStatus(ctx, url, 2*time.Second)
		if err != nil {
			return inspection.Inspection{}, inspectError(err)
		}
	}
	return result, nil
}

func inspectGenerated(ctx context.Context, root string) (inspection.Inspection, inspection.Projection, error) {
	generated, err := bundle.LoadGenerated(ctx, root)
	if err != nil {
		return inspection.Inspection{}, inspection.Projection{}, err
	}
	projection, err := inspection.ProjectManifest(generated.Manifest)
	if err != nil {
		return inspection.Inspection{}, inspection.Projection{}, err
	}
	return summarizeInspection(generated, projection), projection, nil
}

func loadComparisonProjection(ctx context.Context, target string) (inspection.Projection, error) {
	info, err := os.Stat(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return inspection.Projection{}, &Error{Kind: ErrorFilesystem, Code: "COMPARE_TARGET_NOT_FOUND", Message: "comparison target does not exist", Remediation: "Provide a generated bundle directory or a floceed project YAML file.", Err: err}
		}
		return inspection.Projection{}, inspectError(err)
	}
	root := target
	if !info.IsDir() {
		ext := strings.ToLower(filepath.Ext(target))
		if !info.Mode().IsRegular() || (ext != ".yaml" && ext != ".yml") {
			return inspection.Projection{}, &Error{Kind: ErrorUsage, Code: "COMPARE_TARGET_AMBIGUOUS", Message: "comparison target is neither a generated directory nor a project YAML file", Remediation: "Pass the generated directory or its .yaml/.yml project file."}
		}
		file, openErr := os.Open(target)
		if openErr != nil {
			return inspection.Projection{}, inspectError(openErr)
		}
		project, decodeErr := config.Decode(file)
		closeErr := file.Close()
		if decodeErr != nil {
			return inspection.Projection{}, &Error{Kind: ErrorPlan, Code: "COMPARE_PROJECT_INVALID", Message: decodeErr.Error(), Remediation: "Fix the comparison project configuration and retry.", Err: decodeErr}
		}
		if closeErr != nil {
			return inspection.Projection{}, inspectError(closeErr)
		}
		root = filepath.Join(filepath.Dir(target), filepath.FromSlash(project.Output.Directory))
	}
	_, projection, err := inspectGenerated(ctx, root)
	if err != nil {
		return inspection.Projection{}, inspectError(err)
	}
	return projection, nil
}

func summarizeInspection(generated bundle.Generated, projection inspection.Projection) inspection.Inspection {
	manifest := generated.Manifest
	result := inspection.Inspection{
		SchemaVersion: inspection.InspectionSchemaVersion, Valid: true,
		ManifestSchema: manifest.SchemaVersion, BundleIdentity: projection.Digest,
		ToolVersion: manifest.Tool.Version, CapturedAt: manifest.Capture.CapturedAt.Format("2006-01-02T15:04:05Z07:00"),
		Partial: manifest.Capture.Partial, SelectedResources: len(manifest.Selected),
		Source: projection.Source, Target: projection.Target, Governance: projection.Governance,
		Findings: projection.Findings, Operations: projection.Operations,
		Runtime: inspection.Runtime{State: inspection.RuntimeNotRequested},
	}
	if manifest.Capture.CapturedAt.IsZero() {
		result.CapturedAt = ""
	}
	for _, sum := range generated.Checksums.Files {
		result.Artifacts.Files++
		result.Artifacts.Bytes += sum.Size
	}
	snapshots := make(map[string]model.Snapshot, len(manifest.Snapshots))
	for _, snapshot := range manifest.Snapshots {
		snapshots[inspectionResourceKey(snapshot.Resource)] = snapshot
	}
	for _, projected := range projection.Resources {
		resource := inspection.Resource{Identity: projected.Identity, Selected: projected.Selected, Governance: projection.Governance}
		if snapshot, ok := snapshots[inspectionIdentityKey(projected.Identity)]; ok {
			resource.Dataset = summarizeDataset(snapshot)
			resource.Findings = inspection.ProjectFindings(snapshot.Findings)
		}
		result.Resources = append(result.Resources, resource)
	}
	serviceIndexes := make(map[string]int)
	for _, resource := range result.Resources {
		index, exists := serviceIndexes[resource.Identity.Service]
		if !exists {
			index = len(result.Services)
			serviceIndexes[resource.Identity.Service] = index
			result.Services = append(result.Services, inspection.ServiceSummary{Service: resource.Identity.Service})
		}
		summary := &result.Services[index]
		summary.Resources++
		if resource.Selected {
			summary.Selected++
		}
		if resource.Dataset != nil {
			summary.Records += resource.Dataset.Records
			summary.SourceBytes += resource.Dataset.SourceBytes
		}
	}
	return result
}

func summarizeDataset(snapshot model.Snapshot) *inspection.DatasetSummary {
	if snapshot.Dataset != nil {
		return &inspection.DatasetSummary{Format: snapshot.Dataset.Format, Records: snapshot.Dataset.Records, SourceBytes: snapshot.Dataset.SourceBytes, Chunks: len(snapshot.Dataset.Chunks), Resumed: snapshot.Dataset.Resumed}
	}
	if len(snapshot.Data) == 0 {
		return nil
	}
	summary := &inspection.DatasetSummary{Format: "legacy", Chunks: len(snapshot.Data)}
	for _, artifact := range snapshot.Data {
		summary.SourceBytes += artifact.Size
	}
	return summary
}

func inspectionResourceKey(ref model.ResourceRef) string {
	return ref.Service + "\x00" + ref.Type + "\x00" + ref.ID
}

func inspectionIdentityKey(identity inspection.ResourceIdentity) string {
	return identity.Service + "\x00" + identity.Type + "\x00" + identity.ID
}

func inspectError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: ErrorFilesystem, Code: "INSPECTION_CANCELED", Message: err.Error(), Remediation: "Retry inspection when the operation can complete.", Err: err}
	}
	if errors.Is(err, bundle.ErrGeneratedRootMissing) {
		return &Error{Kind: ErrorFilesystem, Code: "BUNDLE_NOT_FOUND", Message: err.Error(), Remediation: "Run floceed pull to generate the bundle, then retry inspection.", Err: err}
	}
	if errors.Is(err, bundle.ErrGeneratedSchema) || errors.Is(err, model.ErrSchema) {
		return &Error{Kind: ErrorPlan, Code: "MANIFEST_SCHEMA_UNSUPPORTED", Message: err.Error(), Remediation: "Use a floceed version that supports this bundle schema.", Err: err}
	}
	if errors.Is(err, bundle.ErrGeneratedPath) {
		return &Error{Kind: ErrorFilesystem, Code: "BUNDLE_PATH_UNSAFE", Message: err.Error(), Remediation: "Regenerate the bundle; generated artifacts must be regular files with safe relative paths.", Err: err}
	}
	return &Error{Kind: ErrorFilesystem, Code: "BUNDLE_INTEGRITY_INVALID", Message: fmt.Sprintf("bundle inspection failed: %v", err), Remediation: "Regenerate the bundle with floceed pull and retry inspection.", Err: err}
}
